/** @file

  A sample plugin to remap requests based on a query parameter

  @section license License

  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <pwd.h>
#include <pthread.h>
#include <unistd.h>
#include <dlfcn.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <search.h>
#include <ts/ts.h>
#include <ts/remap.h>
#include <arpa/inet.h>
#include <ts/experimental.h>

#define LOG_PREFIX "bgcache"

void *troot = NULL;
TSMutex troot_mutex;
int txn_slot;


#define PLUGIN_NAME "backcache"

/* function prototypes */


#if __GNUC__ >= 3
#ifndef likely
#define likely(x)   __builtin_expect (!!(x),1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect (!!(x),0)
#endif
#else
#ifndef likely
#define likely(x)   (x)
#endif
#ifndef unlikely
#define unlikely(x) (x)
#endif
#endif /* #if __GNUC__ >= 3 */


#define INT64_MAX (9223372036854775807LL)

typedef struct
{
    char *effective_url;
    TSMBuffer buf;
    TSMLoc http_hdr_loc;
    struct sockaddr *client_addr;
} RequestInfo;

typedef struct
{
    TSMBuffer buf;
    TSMLoc http_hdr_loc;
    TSHttpParser parser;
    bool parsed;
    TSHttpStatus status;
} ResponseInfo;

typedef struct
{
    TSHttpTxn txn;
    TSCont main_cont;
    bool async_req;
    TSIOBuffer req_io_buf, resp_io_buf;
    TSIOBufferReader req_io_buf_reader, resp_io_buf_reader;
    TSVIO r_vio, w_vio;
    TSVConn vconn;
    RequestInfo *req_info;
    ResponseInfo *resp_info;
    time_t txn_start;
} StateInfo;

static ResponseInfo*
create_response_info(void)
{
    ResponseInfo *resp_info;

    resp_info = (ResponseInfo *)TSmalloc(sizeof(ResponseInfo));

    resp_info->buf = TSMBufferCreate();
    resp_info->http_hdr_loc = TSHttpHdrCreate(resp_info->buf);
    resp_info->parser = TSHttpParserCreate();
    resp_info->parsed = false;

    return resp_info;
}

static void
free_response_info(ResponseInfo *resp_info)
{
    TSHandleMLocRelease(resp_info->buf, TS_NULL_MLOC, resp_info->http_hdr_loc);
    TSMBufferDestroy(resp_info->buf);
    TSHttpParserDestroy(resp_info->parser);
    TSfree(resp_info);

}

static RequestInfo*
create_request_info(TSHttpTxn txn)
{
    RequestInfo *req_info;
    char *url;
    int url_len;
    TSMBuffer buf;
    TSMLoc loc;

    req_info = (RequestInfo *)TSmalloc(sizeof(RequestInfo));

    url = TSHttpTxnEffectiveUrlStringGet(txn, &url_len);
    req_info->effective_url = TSstrndup(url, url_len);
    TSfree(url);

    TSHttpTxnClientReqGet(txn, &buf, &loc);
    req_info->buf = TSMBufferCreate();
    TSHttpHdrClone(req_info->buf, buf, loc, &(req_info->http_hdr_loc));
    TSHandleMLocRelease(buf, TS_NULL_MLOC, loc);

    req_info->client_addr = (struct sockaddr *)TSmalloc(sizeof(struct sockaddr));
    memmove((void *) req_info->client_addr, (void *) TSHttpTxnClientAddrGet(txn), sizeof(struct sockaddr));

    return req_info;
}

static void
free_request_info(RequestInfo *req_info)
{
    TSfree(req_info->effective_url);
    TSHandleMLocRelease(req_info->buf, TS_NULL_MLOC, req_info->http_hdr_loc);
    TSMBufferDestroy(req_info->buf);
    TSfree(req_info->client_addr);
    TSfree(req_info);

}

static int
xstrcmp(const void *a, const void *b)
{
    return strcmp((const char *) a, (const char *) b);
}

static void
parse_response(StateInfo *state)
{
    TSIOBufferBlock block;
    TSParseResult pr = TS_PARSE_CONT;
    int64_t avail;
    char *start;

    block = TSIOBufferReaderStart(state->resp_io_buf_reader);

    while ((pr == TS_PARSE_CONT) && (block != NULL))
    {
        start = (char *) TSIOBufferBlockReadStart(block, state->resp_io_buf_reader, &avail);
        if (avail > 0)
        {
            pr = TSHttpHdrParseResp(state->resp_info->parser, state->resp_info->buf, state->resp_info->http_hdr_loc, (const char **) &start, (const char *) (start + avail));
        }
        block = TSIOBufferBlockNext(block);
    }

    if (pr != TS_PARSE_CONT)
    {
        state->resp_info->status = TSHttpHdrStatusGet(state->resp_info->buf, state->resp_info->http_hdr_loc);
        state->resp_info->parsed = true;
    }

}

static int 
cache_hit_check(TSHttpTxn txn)
{
	int cache_check;
	TSHttpTxnCacheLookupStatusGet(txn, &cache_check);
	return cache_check;
}

static bool
check_range_request_from_header(TSHttpTxn txn)
{
	TSMBuffer req_bufp = NULL;
	TSMLoc hdr_loc;
	TSMLoc field_loc;

	// check htt "range" header
	if (TSHttpTxnClientReqGet(txn, &req_bufp, &hdr_loc) != TS_SUCCESS) {
		return false;
	}

	field_loc = TSMimeHdrFieldFind(req_bufp, hdr_loc, TS_MIME_FIELD_RANGE, -1);
	if (field_loc != TS_NULL_MLOC) {
		TSHandleMLocRelease(req_bufp, hdr_loc, field_loc);
		TSHandleMLocRelease(req_bufp, TS_NULL_MLOC, hdr_loc);
		return true;
	}
	else {
		TSHandleMLocRelease(req_bufp, hdr_loc, field_loc);
		TSHandleMLocRelease(req_bufp, TS_NULL_MLOC, hdr_loc);
		return false;
	}
}

static int
consume_resource(TSCont cont, TSEvent event, void *edata)
{
    StateInfo *state;
    int64_t avail;
    TSVConn vconn;

    vconn = (TSVConn) edata;
    state = (StateInfo *) TSContDataGet(cont);

    switch (event)
    {
        case TS_EVENT_VCONN_WRITE_READY:
            // We shouldn't get here because we specify the exact size of the buffer.
        case TS_EVENT_VCONN_WRITE_COMPLETE:
            break;
        case TS_EVENT_VCONN_READ_READY:

            avail = TSIOBufferReaderAvail(state->resp_io_buf_reader);

            if ((state->resp_info) && !state->resp_info->parsed)
            {
                parse_response(state);
            }

            // Consume data
            avail = TSIOBufferReaderAvail(state->resp_io_buf_reader);
            TSIOBufferReaderConsume(state->resp_io_buf_reader, avail);
            TSVIONDoneSet(state->r_vio, TSVIONDoneGet(state->r_vio) + avail);
            TSVIOReenable(state->r_vio);
            break;
        case TS_EVENT_VCONN_READ_COMPLETE:
        case TS_EVENT_VCONN_EOS:
        case TS_EVENT_VCONN_INACTIVITY_TIMEOUT:
            if (event == TS_EVENT_VCONN_INACTIVITY_TIMEOUT)
            {
                TSVConnAbort(vconn, TS_VC_CLOSE_ABORT);
            }
            else
            {
                if (event == TS_EVENT_VCONN_READ_COMPLETE)
                {
                  TSDebug(LOG_PREFIX, "Read Complete");
                }
                else if (event == TS_EVENT_VCONN_EOS)
                {
                    TSDebug(LOG_PREFIX, "EOS");
                }
                TSVConnClose(state->vconn);
            }

            avail = TSIOBufferReaderAvail(state->resp_io_buf_reader);

            if ((state->resp_info) && !state->resp_info->parsed)
            {
                parse_response(state);
            }

            // Consume data
            avail = TSIOBufferReaderAvail(state->resp_io_buf_reader);
            TSIOBufferReaderConsume(state->resp_io_buf_reader, avail);
            TSVIONDoneSet(state->r_vio, TSVIONDoneGet(state->r_vio) + avail);
            if (state->async_req)
            {
                TSMutexLock(troot_mutex);
                tdelete(state->req_info->effective_url, &troot, xstrcmp);
                TSMutexUnlock(troot_mutex);
            }
            free_request_info(state->req_info);
            if (state->resp_info)
            {
                free_response_info(state->resp_info);
            }
            TSIOBufferReaderFree(state->req_io_buf_reader);
            TSIOBufferDestroy(state->req_io_buf);
            TSIOBufferReaderFree(state->resp_io_buf_reader);
            TSIOBufferDestroy(state->resp_io_buf);
            TSfree(state);
            TSContDestroy(cont);
            break;
        default:
            TSError("Unknown event %d.", event);
            break;
    }

    return 0;
}

static int
fetch_resource(TSCont cont, TSEvent event, void *edata)
{
    StateInfo *state;
    TSCont consume_cont;
    //struct sockaddr_in client_addr;
    TSMLoc connection_hdr_loc, connection_hdr_dup_loc, range_hdr_loc, range_hdr_dup_loc;

    state = (StateInfo *) TSContDataGet(cont);

    //li = (RequestInfo *) edata;
    TSMutexLock(troot_mutex);
    // If already doing async lookup lets just close shop and go home
    if (state->async_req && (tfind(state->req_info->effective_url, &troot, xstrcmp) != NULL))
    {
        TSMutexUnlock(troot_mutex);
        free_request_info(state->req_info);
        TSfree(state);
    }
    // Otherwise lets do the lookup!
    else
    {
      /*
          TSReturnCode ret = TSHttpTxnConfigIntSet(state->txn, TS_CONFIG_HTTP_SHARE_SERVER_SESSIONS, 1); // change config 
          */

        if (state->async_req)
        {
            // Lock in tree
            tsearch(state->req_info->effective_url, &troot, xstrcmp);
        }
        TSMutexUnlock(troot_mutex);
        consume_cont = TSContCreate(consume_resource, NULL);
        TSContDataSet(consume_cont, (void *) state);

        if (state->async_req)
        {
            state->resp_info = NULL;
        }
        else
        {
            state->resp_info = create_response_info();
        }

        range_hdr_loc = TSMimeHdrFieldFind(state->req_info->buf, state->req_info->http_hdr_loc, TS_MIME_FIELD_RANGE, TS_MIME_LEN_RANGE);

        while(range_hdr_loc != TS_NULL_MLOC)
        {

            range_hdr_dup_loc = TSMimeHdrFieldNextDup(state->req_info->buf, state->req_info->http_hdr_loc, range_hdr_loc);
            TSMimeHdrFieldRemove(state->req_info->buf, state->req_info->http_hdr_loc, range_hdr_loc);
            TSMimeHdrFieldDestroy(state->req_info->buf, state->req_info->http_hdr_loc, range_hdr_loc);
            TSHandleMLocRelease(state->req_info->buf, state->req_info->http_hdr_loc, range_hdr_loc);
            range_hdr_loc = range_hdr_dup_loc;
        }

        connection_hdr_loc = TSMimeHdrFieldFind(state->req_info->buf, state->req_info->http_hdr_loc, TS_MIME_FIELD_CONNECTION, TS_MIME_LEN_CONNECTION);

        while(connection_hdr_loc != TS_NULL_MLOC)
        {
            connection_hdr_dup_loc = TSMimeHdrFieldNextDup(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
            TSMimeHdrFieldRemove(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
            TSMimeHdrFieldDestroy(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
            TSHandleMLocRelease(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
            connection_hdr_loc = connection_hdr_dup_loc;
        }

        // This seems to have little effect
        TSMimeHdrFieldCreateNamed(state->req_info->buf, state->req_info->http_hdr_loc, TS_MIME_FIELD_CONNECTION, TS_MIME_LEN_CONNECTION, &connection_hdr_loc);
        TSMimeHdrFieldValueStringInsert(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc, -1, TS_HTTP_VALUE_CLOSE, TS_HTTP_LEN_CLOSE);
        TSMimeHdrFieldAppend(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
        TSHandleMLocRelease(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);

        /*
        TSMimeHdrFieldCreateNamed(state->req_info->buf, state->req_info->http_hdr_loc, TS_MIME_FIELD_CONNECTION, TS_MIME_LEN_CONNECTION, &connection_hdr_loc);
        TSMimeHdrFieldValueStringInsert(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc, -1, TS_HTTP_VALUE_CLOSE, TS_HTTP_LEN_CLOSE);
        TSMimeHdrFieldAppend(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
        TSHandleMLocRelease(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
        */

        state->req_io_buf = TSIOBufferCreate();
        state->req_io_buf_reader = TSIOBufferReaderAlloc(state->req_io_buf);
        state->resp_io_buf = TSIOBufferCreate();
        state->resp_io_buf_reader = TSIOBufferReaderAlloc(state->resp_io_buf);

        TSHttpHdrPrint(state->req_info->buf, state->req_info->http_hdr_loc, state->req_io_buf);
        TSIOBufferWrite(state->req_io_buf, "\r\n", 2);

        state->vconn = TSHttpConnect((struct sockaddr const *) state->req_info->client_addr);

        state->r_vio = TSVConnRead(state->vconn, consume_cont, state->resp_io_buf, INT64_MAX);
        state->w_vio = TSVConnWrite(state->vconn, consume_cont, state->req_io_buf_reader, TSIOBufferReaderAvail(state->req_io_buf_reader));
    }

    TSContDestroy(cont);

    return 0;
}

static int
backcache_plugin(TSCont contp, TSEvent event, void *edata)
{
	TSHttpTxn txn = (TSHttpTxn) edata;
  int lookup_count;
  TSCont fetch_cont;
  StateInfo *state;
  //remap_entry *ri;

	switch (event) {
    case TS_EVENT_HTTP_READ_REQUEST_HDR :
      if (TSHttpIsInternalRequest(txn) != TS_SUCCESS) {
        if (check_range_request_from_header(txn)) {
          StateInfo *state = (StateInfo *)TSmalloc(sizeof(StateInfo));
          time(&state->txn_start);
          state->req_info = create_request_info(txn);
          TSHttpTxnArgSet(txn, txn_slot, (void *) state);
          TSHttpTxnHookAdd(txn, TS_HTTP_CACHE_LOOKUP_COMPLETE_HOOK, contp);
        }
        else {
          TSDebug(LOG_PREFIX, "Not range request. so skip");
        }
      }
      else {
        TSDebug(LOG_PREFIX, "Internal Request"); // This is insufficient if there are other plugins using TSHttpConnect
        TSHttpTxnHookAdd(txn, TS_HTTP_READ_RESPONSE_HDR_HOOK, contp);
      }

      TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
      break;
    case TS_EVENT_HTTP_CACHE_LOOKUP_COMPLETE :
      TSMgmtInt value;
        TSHttpTxnConfigIntGet(txn, TS_CONFIG_HTTP_SHARE_SERVER_SESSIONS, &value);

      //if ((ri = (remap_entry*)TSHttpTxnArgGet(txn, txn_slot))) {
        //state = (StateInfo *)ri->state;
        state = (StateInfo *) TSHttpTxnArgGet(txn, txn_slot);
        TSHttpTxnCacheLookupCountGet(txn, &lookup_count);

        if (cache_hit_check(txn) == TS_CACHE_LOOKUP_MISS) {

          // lookup async
          // Set warning header
          TSHttpTxnHookAdd(txn, TS_HTTP_SEND_RESPONSE_HDR_HOOK, contp);

          state->async_req = true;
          fetch_cont = TSContCreate(fetch_resource, NULL);
          TSContDataSet(fetch_cont, (void *) state);
          TSContSchedule(fetch_cont, 0, TS_THREAD_POOL_TASK);
          TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
          return 0;
        }
        else {
          if (lookup_count == 1)
          {
            free_request_info(state->req_info);
            TSfree(state);
          }
        }
      //}
      TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
      return 0;
    default:
      TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
      break;
  }

  return 0;
}

void
TSPluginInit(int argc, const char *argv[])
{
	TSPluginRegistrationInfo info;
	TSCont cont;

	info.plugin_name = const_cast<char*>(PLUGIN_NAME);
	info.vendor_name = const_cast<char*>("nimbus networks");
	info.support_email = const_cast<char*>("jaekyung.oh@nimbusnetworks.co.kr");
	//info.plugin_version = const_cast<char*>("plugin version : remap_bgcache 1.0");

	if (TSPluginRegister(TS_SDK_VERSION_3_0, &info) != TS_SUCCESS) {
		TSError("[backcache] Plugin registration failed.\n");
		goto Lerror;
	}

  troot_mutex = TSMutexCreate();
	cont = TSContCreate(backcache_plugin, NULL);

	//TSHttpHookAdd(TS_HTTP_CACHE_LOOKUP_COMPLETE_HOOK, cont);
  TSHttpHookAdd(TS_HTTP_READ_REQUEST_HDR_HOOK, cont);

	if (TSHttpArgIndexReserve(PLUGIN_NAME, "backcache in TS_HTTP_READ_REQUEST_HDR_HOOK", &txn_slot) != TS_SUCCESS) {
		TSError("[backcache] failed to reserve private data slot");
		goto Lerror;
	}

  return;

Lerror:
  TSDebug(LOG_PREFIX, "Unable to initialize plugin (disabled).");
}
