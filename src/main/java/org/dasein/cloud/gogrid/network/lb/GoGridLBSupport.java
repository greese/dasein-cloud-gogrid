/**
 * Copyright (C) 2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.gogrid.network.lb;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.gogrid.GoGrid;
import org.dasein.cloud.gogrid.GoGridMethod;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddress;
import org.dasein.cloud.network.LbAlgorithm;
import org.dasein.cloud.network.LbListener;
import org.dasein.cloud.network.LbProtocol;
import org.dasein.cloud.network.LoadBalancer;
import org.dasein.cloud.network.LoadBalancerAddressType;
import org.dasein.cloud.network.LoadBalancerServer;
import org.dasein.cloud.network.LoadBalancerState;
import org.dasein.cloud.network.LoadBalancerSupport;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

/**
 * Twisting of the GoGrid load balancer concept to match the Dasein Cloud load balancer API.
 * <p>Created by George Reese: 10/14/12 10:45 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class GoGridLBSupport implements LoadBalancerSupport {
    static private final Logger logger = GoGrid.getLogger(GoGridLBSupport.class);

    private GoGrid provider;

    public GoGridLBSupport(GoGrid provider) { this.provider = provider; }

    @Override
    public void addDataCenters(String toLoadBalancerId, String... dataCenterIdsToAdd) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void addServers(String toLoadBalancerId, String... serverIdsToAdd) throws CloudException, InternalException {
        if( serverIdsToAdd == null || serverIdsToAdd.length < 1 ) {
            return;
        }
        LoadBalancer lb = getLoadBalancer(toLoadBalancerId);

        if( lb == null ) {
            throw new CloudException("No such load balancer: " + toLoadBalancerId);
        }
        TreeSet<String> serverIds = new TreeSet<String>();

        if( lb.getProviderServerIds() != null ) {
            Collections.addAll(serverIds, lb.getProviderServerIds());
        }
        Collections.addAll(serverIds, serverIdsToAdd);
        edit(lb, serverIds);
    }

    private void edit(@Nonnull LoadBalancer lb, @Nonnull Collection<String> serverIds) throws CloudException, InternalException {
        LbListener[] listeners = lb.getListeners();

        if( listeners == null ) {
            return;
        }

        ArrayList<GoGridMethod.Param> params = new ArrayList<GoGridMethod.Param>();

        params.add(new GoGridMethod.Param("id", lb.getProviderLoadBalancerId()));
        int idx = 1;

        for( String sid : serverIds ) {
            VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(sid);

            if( vm != null ) {
                String ip = vm.getProviderAssignedIpAddressId();

                if( ip != null ) {
                    IpAddress addr = provider.getNetworkServices().getIpAddressSupport().getIpAddress(ip);

                    if( addr != null ) {
                        for( LbListener listener : listeners ) {
                            params.add(new GoGridMethod.Param("realiplist." + idx + ".ip", addr.getAddress()));
                            params.add(new GoGridMethod.Param("realiplist." + idx + ".port", String.valueOf(listener.getPrivatePort())));
                            idx++;
                        }
                    }
                }
            }
        }
        GoGridMethod method = new GoGridMethod(provider);

        if( logger.isDebugEnabled() ) {
            logger.debug("Changing load balancer: " + lb.getProviderLoadBalancerId());
        }
        method.get(GoGridMethod.LB_EDIT, params.toArray(new GoGridMethod.Param[params.size()]));
    }

    @Override
    public String create(String name, String description, String addressId, String[] dataCenterIds, LbListener[] listeners, String[] serverIds) throws CloudException, InternalException {
        IpAddress address = provider.getNetworkServices().getIpAddressSupport().getIpAddress(addressId);

        if( address == null ) {
            throw new CloudException("No such IP address: " + addressId);
        }
        if( address.isAssigned() ) {
            throw new CloudException("IP address is already assigned");
        }
        ArrayList<GoGridMethod.Param> params = new ArrayList<GoGridMethod.Param>();
        LbAlgorithm algorithm = LbAlgorithm.ROUND_ROBIN;
        int publicPort = -1;

        if( listeners != null ) {
            for( LbListener listener : listeners ) {
                if( listener.getPublicPort() > -1 ) {
                    if( publicPort != -1 && listener.getPublicPort() != publicPort ) {
                        throw new CloudException("GoGrid allows only one public port per load balancer");
                    }
                    publicPort = listener.getPublicPort();
                }
                if( listener.getAlgorithm() != null && !algorithm.equals(listener.getAlgorithm()) ) {
                    algorithm = listener.getAlgorithm();
                }
            }
        }
        if( publicPort == -1 ) {
            throw new CloudException("Only TCP is supported");
        }
        if( name == null ) {
            name = "New Load Balancer " + (new Date());
        }
        if( description == null ) {
            description = name;
        }
        params.add(new GoGridMethod.Param("name", name));
        params.add(new GoGridMethod.Param("virtualip.port", String.valueOf(publicPort)));
        params.add(new GoGridMethod.Param("virtualip.ip", addressId));
        params.add(new GoGridMethod.Param("description", description));
        params.add(new GoGridMethod.Param("loadbalancer.type", String.valueOf(algorithm.equals(LbAlgorithm.LEAST_CONN) ? 2 : 1)));
        int idx = 1;

        if( serverIds != null ) {
            for( String sid : serverIds ) {
                VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(sid);

                if( vm != null ) {
                    String ip = vm.getProviderAssignedIpAddressId();

                    if( ip != null ) {
                        IpAddress rip = provider.getNetworkServices().getIpAddressSupport().getIpAddress(ip);

                        if( rip == null ) {
                            throw new CloudException("No such IP address for " + sid + ": " + ip);
                        }
                        for( LbListener listener : listeners ) {
                            params.add(new GoGridMethod.Param("realiplist." + idx + ".ip", rip.getAddress()));
                            params.add(new GoGridMethod.Param("realiplist." + idx + ".port", String.valueOf(listener.getPrivatePort())));
                            idx++;
                        }
                    }
                }
            }
        }
        GoGridMethod method = new GoGridMethod(provider);

        if( logger.isDebugEnabled() ) {
            logger.debug("Creating Load Balancer: " + name);
        }
        JSONArray creates = method.get(GoGridMethod.LB_ADD, params.toArray(new GoGridMethod.Param[params.size()]));

        if( logger.isDebugEnabled() ) {
            logger.debug("create list=" + creates);
            if( creates != null ) {
                logger.debug("size=" + creates.length());
            }
        }
        if( creates != null && creates.length() == 1 ) {
            try {
                JSONObject json = creates.getJSONObject(0);

                if( json != null && json.has("name") ) {
                    name = json.getString("name");
                }
                long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 15L);

                while( json != null && !json.has("id") ) {
                    if( System.currentTimeMillis() > timeout ) {
                        throw new CloudException("Timed out waiting for GoGrid to provide a load balancer ID");
                    }
                    try { Thread.sleep(30000L); }
                    catch( InterruptedException ignore ) { }
                    creates = method.get(GoGridMethod.LB_GET, new GoGridMethod.Param("name", name));
                    if( creates != null && creates.length() == 1 ) {
                        json = creates.getJSONObject(0);
                    }
                }
                LoadBalancer lb = toLoadBalancer(json, method.get(GoGridMethod.SERVER_LIST));

                if( lb != null ) {
                    if( logger.isDebugEnabled() ) {
                        logger.debug("LB=" + lb);
                    }
                    return lb.getProviderLoadBalancerId();
                }
            }
            catch( JSONException e ) {
                logger.error("Launches did not come back in the form of a valid list: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        throw new CloudException("Action succeeded, but no load balancer is shown");
    }

    @Override
    public LoadBalancer getLoadBalancer(String loadBalancerId) throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.LB_GET, new GoGridMethod.Param("id", loadBalancerId));

        if( list == null ) {
            return null;
        }

        JSONArray servers = method.get(GoGridMethod.SERVER_LIST);

        for( int i=0; i<list.length(); i++ ) {
            try {
                LoadBalancer lb = toLoadBalancer(list.getJSONObject(i), servers);

                if( lb != null && lb.getProviderLoadBalancerId().equals(loadBalancerId) ) {
                    return lb;
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        return null;
    }

    @Override
    public Iterable<LoadBalancerServer> getLoadBalancerServerHealth(String loadBalancerId) throws CloudException, InternalException {
        return Collections.emptyList(); // TODO: implement this
    }

    @Override
    public Iterable<LoadBalancerServer> getLoadBalancerServerHealth(String loadBalancerId, String... serverIdsToCheck) throws CloudException, InternalException {
        return Collections.emptyList(); // todo: implement this
    }

    @Override
    public LoadBalancerAddressType getAddressType() throws CloudException, InternalException {
        return LoadBalancerAddressType.IP;
    }

    private @Nonnull ProviderContext getContext() throws CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was provided for this request");
        }
        return ctx;
    }

    @Override
    public int getMaxPublicPorts() throws CloudException, InternalException {
        return 1;
    }

    @Override
    public String getProviderTermForLoadBalancer(Locale locale) {
        return "load balancer";
    }

    @Override
    public Iterable<ResourceStatus> listLoadBalancerStatus() throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        String regionId = getRegionId(ctx);

        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.LB_LIST, new GoGridMethod.Param("datacenter", regionId));

        if( list == null ) {
            return Collections.emptyList();
        }
        ArrayList<ResourceStatus> loadBalancers = new ArrayList<ResourceStatus>();

        for( int i=0; i<list.length(); i++ ) {
            try {
                ResourceStatus lb = toStatus(list.getJSONObject(i));

                if( lb != null ) {
                    loadBalancers.add(lb);
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        return loadBalancers;
    }

    private @Nonnull String getRegionId(@Nonnull ProviderContext ctx) throws CloudException {
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was provided for this request");
        }
        return regionId;
    }

    static private volatile List<LbAlgorithm> algorithms = null;

    @Override
    public Iterable<LbAlgorithm> listSupportedAlgorithms() throws CloudException, InternalException {
        List<LbAlgorithm> list = algorithms;

        if( list == null ) {
            list = new ArrayList<LbAlgorithm>();
            list.add(LbAlgorithm.ROUND_ROBIN);
            list.add(LbAlgorithm.LEAST_CONN);
            algorithms = Collections.unmodifiableList(list);
        }
        return algorithms;
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Override
    public Iterable<LbProtocol> listSupportedProtocols() throws CloudException, InternalException {
        return Collections.singletonList(LbProtocol.RAW_TCP);
    }

    @Override
    public boolean isAddressAssignedByProvider() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isDataCenterLimited() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean requiresListenerOnCreate() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean requiresServerOnCreate() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);
        String regionId = getRegionId(getContext());

        JSONArray list = method.get(GoGridMethod.LOOKUP_LIST, new GoGridMethod.Param("lookup", "loadbalancer.datacenter"));

        if( list == null ) {
            return false;
        }
        for( int i=0; i<list.length(); i++ ) {
            try {
                JSONObject r = list.getJSONObject(i);

                if( r.has("id") && regionId.equals(r.getString("id")) ) {
                    return true;
                }
            }
            catch( JSONException e ) {
                logger.error("Unable to load data centers from GoGrid: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        return false;
    }

    @Override
    public boolean supportsMonitoring() throws CloudException, InternalException {
        return false;
    }

    @Override
    public Iterable<LoadBalancer> listLoadBalancers() throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        String regionId = getRegionId(ctx);

        GoGridMethod method = new GoGridMethod(provider);

        JSONArray servers = method.get(GoGridMethod.SERVER_LIST);
        JSONArray list = method.get(GoGridMethod.LB_LIST, new GoGridMethod.Param("datacenter", regionId));

        if( list == null ) {
            return Collections.emptyList();
        }
        ArrayList<LoadBalancer> loadBalancers = new ArrayList<LoadBalancer>();

        for( int i=0; i<list.length(); i++ ) {
            try {
                LoadBalancer lb = toLoadBalancer(list.getJSONObject(i), servers);

                if( lb != null ) {
                    loadBalancers.add(lb);
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        return loadBalancers;
    }

    @Override
    public void remove(String loadBalancerId) throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);

        method.get(GoGridMethod.LB_DELETE, new GoGridMethod.Param("id", loadBalancerId));
    }

    @Override
    public void removeDataCenters(String fromLoadBalancerId, String... dataCenterIdsToRemove) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No support for removing data centers from a load balancer in GoGrid.");
    }

    @Override
    public void removeServers(String fromLoadBalancerId, String... serverIdsToRemove) throws CloudException, InternalException {
        if( serverIdsToRemove == null || serverIdsToRemove.length < 1 ) {
            return;
        }
        LoadBalancer lb = getLoadBalancer(fromLoadBalancerId);

        if( lb == null ) {
            throw new CloudException("No such load balancer: " + fromLoadBalancerId);
        }
        TreeSet<String> serverIds = new TreeSet<String>();

        if( lb.getProviderServerIds() != null ) {
            Collections.addAll(serverIds, lb.getProviderServerIds());
        }
        for( String id : serverIdsToRemove ) {
            serverIds.remove(id);
        }
        edit(lb, serverIds);
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    /*
    {"id":1,"description":"","name":"Round Robin","object":"option"},
    {"id":2,"description":"","name":"Least Connect","object":"option"}
     */
    private @Nonnull LbAlgorithm toAlgorithm(int type) {
        switch( type ) {
            case 1: return LbAlgorithm.ROUND_ROBIN;
            case 2: return LbAlgorithm.LEAST_CONN;
        }
        return LbAlgorithm.ROUND_ROBIN;
    }

    private @Nullable ResourceStatus toStatus(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }

        LoadBalancerState state = null;
        String loadBalancerId = null;

        try {
            if( json.has("id") ) {
                loadBalancerId = json.getString("id");
            }
            if( json.has("state") ) {
                /*
                {"id":1,"description":"Loadbalancer is enabled and on.","name":"On","object":"option"},
                {"id":2,"description":"Loadbalancer is disabled and off.","name":"Off","object":"option"},
                {"id":3,"description":"Loadbalancer is enabled, but real ips are unreachable.","name":"Unavailable","object":"option"},
                {"id":4,"description":"Loadbalancer state is unknown.","name":"Unknown","object":"option"}
                 */
                JSONObject s = json.getJSONObject("state");

                if( s.has("id") ) {
                    int id = s.getInt("id");

                    switch( id ) {
                        case 1: case 3: state = LoadBalancerState.ACTIVE; break;
                        case 2: case 4: state = LoadBalancerState.PENDING; break;
                    }
                }
            }
        }
        catch( JSONException e ) {
            logger.error("Failed to parse load balancer JSON from cloud: " + e.getMessage());
            e.printStackTrace();
            throw new CloudException(e);
        }
        if( loadBalancerId == null ) {
            return null;
        }
        if( state == null ) {
            state = LoadBalancerState.PENDING;
        }
        return new ResourceStatus(loadBalancerId, state);
    }

    private @Nullable LoadBalancer toLoadBalancer(@Nullable JSONObject json, @Nullable JSONArray servers) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }

        LoadBalancer lb = new LoadBalancer();
        String regionId = getRegionId(getContext());

        lb.setProviderOwnerId(getContext().getAccountNumber());
        lb.setProviderRegionId(regionId);
        lb.setSupportedTraffic(new IPVersion[] { IPVersion.IPV4 });
        lb.setCurrentState(LoadBalancerState.PENDING);
        lb.setAddressType(LoadBalancerAddressType.IP);
        try {
            LbAlgorithm algorithm = LbAlgorithm.ROUND_ROBIN;
            int publicPort = -1;

            if( json.has("type") ) {
                JSONObject type = json.getJSONObject("type");

                if( type.has("id") ) {
                    algorithm = toAlgorithm(type.getInt("id"));
                }
            }
            if( json.has("id") ) {
                lb.setProviderLoadBalancerId(json.getString("id"));
            }
            if( json.has("name") ) {
                lb.setName(json.getString("name"));
            }
            if( json.has("description") ) {
                lb.setDescription(json.getString("description"));
            }
            if( json.has("datacenter") ) {
                JSONObject dc = json.getJSONObject("datacenter");

                if( dc.has("id") ) {
                    lb.setProviderRegionId(dc.getString("id"));
                    if( !regionId.equals(lb.getProviderRegionId()) ) {
                        return null;
                    }
                }
            }
            if( json.has("state") ) {
                /*
                {"id":1,"description":"Loadbalancer is enabled and on.","name":"On","object":"option"},
                {"id":2,"description":"Loadbalancer is disabled and off.","name":"Off","object":"option"},
                {"id":3,"description":"Loadbalancer is enabled, but real ips are unreachable.","name":"Unavailable","object":"option"},
                {"id":4,"description":"Loadbalancer state is unknown.","name":"Unknown","object":"option"}
                 */
                JSONObject state = json.getJSONObject("state");

                if( state.has("id") ) {
                    int id = state.getInt("id");

                    switch( id ) {
                        case 1: case 3: lb.setCurrentState(LoadBalancerState.ACTIVE); break;
                        case 2: case 4: lb.setCurrentState(LoadBalancerState.PENDING); break;
                    }
                }
            }
            if( json.has("virtualip") ) {
                JSONObject vip = json.getJSONObject("virtualip");

                if( vip.has("ip") ) {
                    JSONObject ip = vip.getJSONObject("ip");

                    if( ip.has("ip") ) {
                        lb.setAddress(ip.getString("ip"));
                    }
                }
                if( vip.has("port") ) {
                    publicPort = vip.getInt("port");
                    lb.setPublicPorts(new int[] { publicPort });
                }
            }
            if( json.has("realiplist") ) {
                ArrayList<LbListener> listeners = new ArrayList<LbListener>();
                TreeSet<String> serverIds = new TreeSet<String>();
                JSONArray ips = json.getJSONArray("realiplist");

                for( int i=0; i<ips.length(); i++ ) {
                    JSONObject ip = ips.getJSONObject(i);
                    LbListener listener = new LbListener();

                    listener.setPublicPort(publicPort);
                    listener.setAlgorithm(algorithm);
                    listener.setNetworkProtocol(LbProtocol.RAW_TCP);
                    if( ip.has("port") ) {
                        listener.setPrivatePort(ip.getInt("port"));
                    }
                    if( ip.has("ip") ) {
                        JSONObject address = ip.getJSONObject("ip");

                        if( address.has("id") && servers != null ) {
                            String id = address.getString("id");

                            for( int j=0; j<servers.length(); j++ ) {
                                JSONObject server = servers.getJSONObject(j);
                                String serverId = null;

                                if( server.has("privateip") ) {
                                    JSONObject sip = server.getJSONObject("privateip");

                                    if( sip.has("id") && id.equals(sip.getString("id")) && server.has("id") ) {
                                        serverId = server.getString("id");
                                    }
                                }
                                if( serverId == null && server.has("ip") ) {
                                    JSONObject sip = server.getJSONObject("ip");

                                    if( sip.has("id") && id.equals(sip.getString("id")) && server.has("id") ) {
                                        serverId = server.getString("id");
                                    }
                                }
                                if( serverId != null ) {
                                    serverIds.add(serverId);
                                    break;
                                }
                            }
                        }
                    }
                    listeners.add(listener);
                }
                lb.setProviderServerIds(serverIds.toArray(new String[serverIds.size()]));
                lb.setListeners(listeners.toArray(new LbListener[listeners.size()]));
            }
        }
        catch( JSONException e ) {
            logger.error("Failed to parse load balancer JSON from cloud: " + e.getMessage());
            e.printStackTrace();
            throw new CloudException(e);
        }
        if( lb.getProviderLoadBalancerId() == null ) {
            return null;
        }
        if( lb.getName() == null ) {
            lb.setName(lb.getProviderLoadBalancerId());
        }
        if( lb.getDescription() == null ) {
            lb.setDescription(lb.getName());
        }
        return lb;
    }
}
