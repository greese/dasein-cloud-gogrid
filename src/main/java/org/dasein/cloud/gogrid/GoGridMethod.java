/**
 * ========= CONFIDENTIAL =========
 *
 * Copyright (C) 2012 enStratus Networks Inc - ALL RIGHTS RESERVED
 *
 * ====================================================================
 *  NOTICE: All information contained herein is, and remains the
 *  property of enStratus Networks Inc. The intellectual and technical
 *  concepts contained herein are proprietary to enStratus Networks Inc
 *  and may be covered by U.S. and Foreign Patents, patents in process,
 *  and are protected by trade secret or copyright law. Dissemination
 *  of this information or reproduction of this material is strictly
 *  forbidden unless prior written permission is obtained from
 *  enStratus Networks Inc.
 * ====================================================================
 */
package org.dasein.cloud.gogrid;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Represents the interaction point between Dasein Cloud and the underlying REST API.
 * <p>Created by George Reese: 10/13/12 3:31 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class GoGridMethod {
    static public class Param {
        private String key;
        private String value;

        public Param(@Nonnull String key, @Nonnull String value) {
            this.key = key;
            this.value = value;
        }

        public @Nonnull String getKey() {
            return key;
        }

        public @Nonnull String getValue() {
            return value;
        }
    }

    static public final String LOOKUP_LIST = "/api/common/lookup/list";

    static public final String VERSION = "1.9";

    static private final Logger logger = GoGrid.getLogger(GoGridMethod.class);
    static private final Logger wire = GoGrid.getWireLogger(GoGridMethod.class);

    /**
     * {"summary":{"total":24,"start":0,"numpages":0,"returned":24},"status":"success","method":"/common/lookup/list",
     * "list":[{"id":1,"description":"Load Balancer Types","name":"loadbalancer.type","object":"option"},
     * {"id":2,"description":"Load Balancer Persistence Types","name":"loadbalancer.persistence","object":"option"},
     * {"id":3,"description":"Load Balancer States","name":"loadbalancer.state","object":"option"},
     * {"id":4,"description":"Load Balancer OS's","name":"loadbalancer.os","object":"option"},
     * {"id":5,"description":"Server Types","name":"server.type","object":"option"},
     * {"id":6,"description":"Server Ram","name":"server.ram","object":"option"},
     * {"id":7,"description":"Server States","name":"server.state","object":"option"},
     * {"id":8,"description":"Server OS's","name":"server.os","object":"option"},
     * {"id":9,"description":"IP Address Types","name":"ip.type","object":"option"},
     * {"id":10,"description":"IP Address States","name":"ip.state","object":"option"},
     * {"id":11,"description":"Job States","name":"job.state","object":"option"},
     * {"id":12,"description":"Job Commands","name":"job.command","object":"option"},
     * {"id":13,"description":"Job Object Types","name":"job.objecttype","object":"option"},
     * {"id":14,"description":"Image States","name":"image.state","object":"option"},
     * {"id":15,"description":"Image Types","name":"image.type","object":"option"},
     * {"id":16,"description":"Image OS's","name":"image.os","object":"option"},
     * {"id":17,"description":"Image OS Architectures","name":"image.architecture","object":"option"},
     * {"id":18,"description":"Datacenters","name":"datacenter","object":"option"},
     * {"id":19,"description":"Datacenters","name":"loadbalancer.datacenter","object":"option"},
     * {"id":20,"description":"Datacenters","name":"server.datacenter","object":"option"},
     * {"id":21,"description":"Datacenters","name":"job.datacenter","object":"option"},
     * {"id":22,"description":"Datacenters","name":"ip.datacenter","object":"option"},
     * {"id":23,"description":"Image Ram","name":"image.minram","object":"option"},
     * {"id":24,"description":"GoGrid Server Image Type","name":"image.gsitype","object":"option"}]}
     */
    private GoGrid provider;

    public GoGridMethod(@Nonnull GoGrid provider) { this.provider = provider; }

    private @Nullable Object doGet(boolean asList, @Nonnull String service, @Nullable Param ... params) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + GoGrid.class.getName() + ".doGet(" + service + "," + Arrays.toString(params) + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [GET/" + asList + "] -> " + service + " >--------------------------------------------------------------------------------------");
        }
        try {
            ProviderContext ctx = provider.getContext();

            if( ctx == null ) {
                throw new CloudException("No context was set for this request");
            }
            String key;

            try {
                key = new String(ctx.getAccessPublic(), "utf-8");
            }
            catch( UnsupportedEncodingException e ) {
                e.printStackTrace();
                throw new InternalException("UTF-8 not supported");
            }
            String endpoint = getEndpoint(ctx, service);
            String signature = sign(ctx);

            String paramString = "?v=" + VERSION + "&api_key=" + key + "&sig=" + signature;

            if( params != null && params.length > 0 ) {
                for( Param p : params ) {
                    paramString = paramString + "&" + p.getKey() + "=" + p.getValue();
                }
            }
            HttpGet get = new HttpGet(endpoint + paramString);
            HttpClient client = getClient(ctx, endpoint.startsWith("https"));

            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                e.printStackTrace();
                throw new InternalException(e);
            }
            int status = response.getStatusLine().getStatusCode();

            if( status == HttpServletResponse.SC_NOT_FOUND ) {
                return null;
            }
            if( status == HttpServletResponse.SC_OK ) {
                HttpEntity entity = response.getEntity();
                String json;

                if( entity == null ) {
                    return null;
                }
                try {
                    json = EntityUtils.toString(entity);
                    if( wire.isDebugEnabled() ) {
                        wire.debug(json);
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read JSON entity");
                    e.printStackTrace();
                    throw new CloudException(e);
                }
                try {
                    JSONObject r = new JSONObject(json);

                    if( asList && r.has("list") ) {
                        return r.getJSONArray("list");
                    }
                    else if( !asList ) {
                        return r;
                    }
                    return null;
                }
                catch( JSONException e ) {
                    logger.error("Invalid JSON from cloud: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
            }
            throw new GoGridException(new GoGridException.ParsedException(response));
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + GoGridMethod.class.getName() + ".doGet()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [GET/" + asList + "] -> " + service + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }

    public @Nullable JSONObject get(@Nonnull String service, @Nullable Param ... params) throws CloudException, InternalException {
        return (JSONObject)doGet(false, service, params);
    }

    public @Nullable JSONArray list(@Nonnull String service, @Nullable Param ... params) throws CloudException, InternalException {
        return (JSONArray)doGet(true, service, params);
    }

    private @Nonnull HttpClient getClient(@Nonnull ProviderContext ctx, boolean ssl) {
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        //noinspection deprecation
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "Dasein Cloud");

        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if( proxyHost != null ) {
                int port = 0;

                if( proxyPort != null && proxyPort.length() > 0 ) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        return new DefaultHttpClient(params);
    }

    public @Nonnull String getEndpoint(@Nonnull ProviderContext ctx, @Nonnull String service) {
        String endpoint = ctx.getEndpoint();

        if( endpoint == null || endpoint.equals("") ) {
            endpoint = "http://api.gogrid.com";
        }
        if( endpoint.endsWith("/") && service.startsWith("/") ) {
            while( endpoint.endsWith("/") && !endpoint.equals("/") ) {
                endpoint = endpoint.substring(0, endpoint.length()-1);
            }
        }
        if( service.startsWith("/") || endpoint.endsWith("/") ) {
            return endpoint + service;
        }
        return endpoint + service;
    }

    private @Nonnull String sign(@Nonnull ProviderContext ctx) throws CloudException, InternalException {
        byte[] publicKey = ctx.getAccessPublic();
        byte[] privateKey = ctx.getAccessPrivate();

        try {
            String toSign = (new String(publicKey, "utf-8")) + (new String(privateKey, "utf-8")) + (System.currentTimeMillis()/1000);

            logger.info("String to sign=" + toSign);

            String signature = stupidPHPMD5(toSign);

            logger.info("Signature=" + signature);
            return signature;
        }
        catch( UnsupportedEncodingException e ) {
            e.printStackTrace();
            throw new InternalException("UTF-8 not supported");
        }
    }

    private @Nonnull String hex(@Nonnull byte[] array) {
        StringBuilder str = new StringBuilder();

        for( byte b : array ) {
            str.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1, 3));
        }
        return str.toString();
    }

    private @Nonnull String stupidPHPMD5(@Nonnull String toSign) throws InternalException, CloudException{
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");

            return hex(digest.digest(toSign.getBytes("CP1252")));
        }
        catch( NoSuchAlgorithmException e ) {
            logger.error("No support for MD5: " + e.getMessage());
            e.printStackTrace();
            throw new InternalException(e);
        }
        catch( UnsupportedEncodingException e ) {
            logger.error("No support for CP1252: " + e.getMessage());
            e.printStackTrace();
            throw new InternalException(e);
        }
    }
}
