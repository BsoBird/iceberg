/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.lock;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.ResponseContentEncoding;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.iceberg.util.LockManagers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** User. */
public class ServerSideHttpLockManager extends LockManagers.BaseLockManager{
    private static final Logger LOG = LoggerFactory.getLogger(ServerSideHttpLockManager.class);
    private String httpUrl = null;
    private final static String OPERATOR = "operator";
    private final static String LOCK = "lock";
    private final static String UNLOCK = "unlock";
    private final static String ENTITY_ID = "entityId";
    private final static String OWNER_ID = "ownerId";
    private final static int REQUEST_SUCCESS = 200;
    public final static String REQUEST_URL = "lock.http.conf.request.url";

    private final static String params = "ownerId=${ownerId}&entityId=${entityId}&operator=${operator}";
    private HttpClient httpClient = null;
    public ServerSideHttpLockManager(){}

    public ServerSideHttpLockManager(String requestUrl){
        init(requestUrl);
    }

    @Override
    public boolean acquire(String entityId, String ownerId) {
        return process(entityId,ownerId,LOCK);
    }

    @Override
    public void initialize(Map<String, String> properties) {
        super.initialize(properties);
        String httpUrl = properties.getOrDefault(REQUEST_URL,null);
        init(httpUrl);
    }

    private synchronized void init(String requestUrl){
        if(requestUrl==null){
            String msg = String.format("[%s] must be set.",REQUEST_URL);
            throw new IllegalArgumentException(msg);
        }
        if(this.httpUrl==null){
            this.httpUrl = requestUrl;
        }
        if(this.httpClient==null){
            DefaultHttpClient defaultHttpClient = new DefaultHttpClient();
            defaultHttpClient.addRequestInterceptor(new RequestAcceptEncoding());
            defaultHttpClient.addResponseInterceptor(new ResponseContentEncoding());
            this.httpClient = new DefaultHttpClient();
        }
    }

    private String encode(String entity){
        if(entity==null){
            return null;
        }
        return new String(Base64.getUrlEncoder().encode(entity.getBytes(StandardCharsets.UTF_8)),StandardCharsets.UTF_8);
    }

    private boolean process(String entityId, String ownerId, String operator){
        try{
            HttpGet lockRequest = new HttpGet();
            lockRequest.addHeader("Content-Type","application/json");
            HttpParams requestParams = new BasicHttpParams();
            requestParams.setParameter(OWNER_ID,encode(ownerId));
            requestParams.setParameter(ENTITY_ID,encode(entityId));
            requestParams.setParameter(OPERATOR,operator);
            String requestUrl = new URIBuilder(httpUrl)
                    .setParameter(OWNER_ID, encode(ownerId))
                    .setParameter(ENTITY_ID,encode(entityId))
                    .setParameter(OPERATOR,operator)
                    .build().toString();
            lockRequest.setParams(requestParams);
            lockRequest.setURI(new URI(requestUrl));
            HttpResponse response = httpClient.execute(lockRequest);
            StatusLine stateLine = response.getStatusLine();
            int statsCode = stateLine.getStatusCode();
            lockRequest.abort();
            return REQUEST_SUCCESS == statsCode;
        }catch (URISyntaxException e){
            throw new RuntimeException(e);
        }catch (Exception e){
            LOG.error("An exception occurred during the {} process.",operator,e);
        }
        return false;
    }


    @Override
    public boolean release(String entityId, String ownerId) {
        return process(entityId,ownerId,UNLOCK);
    }
}
