/**
 * Copyright (C) 2012-2013 enStratus Networks Inc
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

package org.dasein.cloud.gogrid.network.ip;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.gogrid.GoGrid;
import org.dasein.cloud.gogrid.GoGridMethod;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.AddressType;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddress;
import org.dasein.cloud.network.IPAddressCapabilities;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.IpForwardingRule;
import org.dasein.cloud.network.Protocol;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

/**
 * Implements services in support for GoGrid IP addresses.
 * <p>Created by George Reese: 10/14/12 11:31 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class GoGridIPSupport implements IpAddressSupport {
    static private final Logger logger = GoGrid.getLogger(GoGridIPSupport.class);

    private GoGrid provider;

    public GoGridIPSupport(GoGrid provider) { this.provider = provider; }

    @Override
    public void assign(@Nonnull String addressId, @Nonnull String serverId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("GoGrid does not support runtime IP assignment");
    }

    @Override
    public void assignToNetworkInterface(@Nonnull String addressId, @Nonnull String nicId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("GoGrid does not support network interfaces");
    }

    @Override
    public @Nonnull String forward(@Nonnull String addressId, int publicPort, @Nonnull Protocol protocol, int privatePort, @Nonnull String onServerId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("GoGrid does not support IP forwarding");
    }

    private transient volatile IPCapabilities capabilities;
    @Nonnull
    @Override
    public IPAddressCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new IPCapabilities(provider);
        }
        return capabilities;
    }

    private @Nonnull ProviderContext getContext() throws CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was provided for this request");
        }
        return ctx;
    }

    @Override
    public IpAddress getIpAddress(@Nonnull String addressId) throws InternalException, CloudException {
        // TODO: optimize
        for( IpAddress addr : listIpPool(IPVersion.IPV4, false) ) {
            if( addr.getProviderIpAddressId().equals(addressId) ) {
                return addr;
            }
        }
        return null;
    }

    private @Nonnull String getRegionId(@Nonnull ProviderContext ctx) throws CloudException {
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was provided for this request");
        }
        return regionId;
    }

    @Override
    public @Nonnull String getProviderTermForIpAddress(@Nonnull Locale locale) {
        return "IP Address";
    }

    @Override
    public @Nonnull Requirement identifyVlanForVlanIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean isAssigned(@Nonnull AddressType type) {
        return false;
    }

    @Override
    public boolean isAssigned(@Nonnull IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isAssignablePostLaunch(@Nonnull IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isForwarding() {
        return false;
    }

    @Override
    public boolean isForwarding(IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isRequestable(@Nonnull AddressType type) {
        return false;
    }

    @Override
    public boolean isRequestable(@Nonnull IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);
        String regionId = getRegionId(getContext());

        JSONArray list = method.get(GoGridMethod.LOOKUP_LIST, new GoGridMethod.Param("lookup", "ip.datacenter"));

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

    // TODO: 1 is public, 2 is private
    @Override
    public @Nonnull Iterable<IpAddress> listPrivateIpPool(boolean unassignedOnly) throws InternalException, CloudException {
        ArrayList<IpAddress> matches = new ArrayList<IpAddress>();

        for( IpAddress addr : listIpPool(IPVersion.IPV4, unassignedOnly) ) {
            if( addr.getAddressType().equals(AddressType.PRIVATE) ) {
                matches.add(addr);
            }
        }
        return matches;
    }

    @Override
    public @Nonnull Iterable<IpAddress> listPublicIpPool(boolean unassignedOnly) throws InternalException, CloudException {
        ArrayList<IpAddress> matches = new ArrayList<IpAddress>();

        for( IpAddress addr : listIpPool(IPVersion.IPV4, unassignedOnly) ) {
            if( addr.getAddressType().equals(AddressType.PUBLIC) ) {
                matches.add(addr);
            }
        }
        return matches;
    }

    @Override
    public @Nonnull Iterable<IpAddress> listIpPool(@Nonnull IPVersion version, boolean unassignedOnly) throws InternalException, CloudException {
        if( version.equals(IPVersion.IPV4) ) {
            ProviderContext ctx = getContext();
            String regionId = getRegionId(ctx);

            GoGridMethod method = new GoGridMethod(provider);

            GoGridMethod.Param[] params = new GoGridMethod.Param[unassignedOnly ? 2 : 1];

            params[0] = new GoGridMethod.Param("datacenter", regionId);
            if( unassignedOnly ) {
                params[1] = new GoGridMethod.Param("ip.state", "1");
            }
            JSONArray list = method.get(GoGridMethod.IP_LIST, params);

            if( list == null ) {
                return Collections.emptyList();
            }
            ArrayList<IpAddress> addresses = new ArrayList<IpAddress>();
            JSONArray vmList = null;
            JSONArray lbList = null;

            if( !unassignedOnly ) {
                vmList = method.get(GoGridMethod.SERVER_LIST);
                lbList = method.get(GoGridMethod.LB_LIST);
            }
            for( int i=0; i<list.length(); i++ ) {
                try {
                    IpAddress ip = toAddress(list.getJSONObject(i), vmList, lbList);

                    if( ip != null ) {
                        addresses.add(ip);
                    }
                }
                catch( JSONException e ) {
                    logger.error("Failed to parse JSON: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
            }
            return addresses;
        }
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listIpPoolStatus(@Nonnull IPVersion version) throws InternalException, CloudException {
        if( version.equals(IPVersion.IPV4) ) {
            ProviderContext ctx = getContext();
            String regionId = getRegionId(ctx);

            GoGridMethod method = new GoGridMethod(provider);

            GoGridMethod.Param[] params = new GoGridMethod.Param[1];

            params[0] = new GoGridMethod.Param("datacenter", regionId);
            JSONArray list = method.get(GoGridMethod.IP_LIST, params);

            if( list == null ) {
                return Collections.emptyList();
            }
            ArrayList<ResourceStatus> addresses = new ArrayList<ResourceStatus>();
            JSONArray vmList = method.get(GoGridMethod.SERVER_LIST);
            JSONArray lbList = method.get(GoGridMethod.LB_LIST);

            for( int i=0; i<list.length(); i++ ) {
                try {
                    ResourceStatus ip = toStatus(list.getJSONObject(i), vmList, lbList);

                    if( ip != null ) {
                        addresses.add(ip);
                    }
                }
                catch( JSONException e ) {
                    logger.error("Failed to parse JSON: " + e.getMessage());
                    e.printStackTrace();
                    throw new CloudException(e);
                }
            }
            return addresses;
        }
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<IpForwardingRule> listRules(@Nonnull String addressId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singleton(IPVersion.IPV4);
    }

    @Override
    public void releaseFromPool(@Nonnull String addressId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("GoGrid does not support request/release from pool");
    }

    @Override
    public void releaseFromServer(@Nonnull String addressId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("GoGrid does not support runtime IP address manipulation");
    }

    @Override
    public @Nonnull String request(@Nonnull AddressType typeOfAddress) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Requesting of IP addresses is not supported in GoGrid");
    }

    @Override
    public @Nonnull String request(@Nonnull IPVersion version) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Requesting of IP addresses is not supported in GoGrid");
    }

    @Override
    public @Nonnull String requestForVLAN(@Nonnull IPVersion version) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Requesting of IP addresses is not supported in GoGrid");
    }

    @Override
    public @Nonnull String requestForVLAN(@Nonnull IPVersion version, @Nonnull String vlanId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Requesting of IP addresses is not supported");
    }

    @Override
    public void stopForward(@Nonnull String ruleId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Forwarding of IP addresses is not supported in GoGrid");
    }

    @Override
    public boolean supportsVLANAddresses(@Nonnull IPVersion ofVersion) throws InternalException, CloudException {
        return false;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable IpAddress toAddress(@Nullable JSONObject json, @Nullable JSONArray vmList, @Nullable JSONArray lbList) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        IpAddress address = new IpAddress();

        address.setForVlan(false);
        address.setRegionId(getRegionId(getContext()));
        address.setVersion(IPVersion.IPV4);
        try {
            if( json.has("id") && json.has("ip") ) {
                address.setIpAddressId(json.getString("id"));
                address.setAddress(json.getString("ip"));
            }
            else {
                return null;
            }
            if( json.has("public") && json.getBoolean("public") ) {
                address.setAddressType(AddressType.PUBLIC);
            }
            else {
                address.setAddressType(AddressType.PRIVATE);
            }
            if( json.has("state") ) {
                JSONObject state = json.getJSONObject("state");

                if( state.has("id") ) {
                    int s = state.getInt("id");

                    if( s != 1 && (vmList == null || lbList == null) ) {
                        return null;
                    }
                    else if( s == 2 ) {
                        for( int i =0; i<vmList.length(); i++ ) {
                            JSONObject vm = vmList.getJSONObject(i);

                            if( !vm.has("id") ) {
                                continue;
                            }
                            if( vm.has("ip") ) {
                                JSONObject ip = vm.getJSONObject("ip");

                                if( ip.has("id") && ip.getString("id").equals(address.getProviderIpAddressId()) ) {
                                    address.setServerId(vm.getString("id"));
                                }
                            }
                        }
                        if( address.getServerId() == null ) {
                            for( int i=0; i<lbList.length(); i++ ) {
                                JSONObject lb = lbList.getJSONObject(i);

                                if( !lb.has("id") ) {
                                    continue;
                                }
                                if( lb.has("virtualip.ip") ) {
                                    JSONObject ip = lb.getJSONObject("virtualip.ip");


                                    if( ip.has("id") && ip.getString("id").equals(address.getProviderIpAddressId()) ) {
                                        address.setProviderLoadBalancerId(lb.getString("id"));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        catch( JSONException e ) {
            logger.error("Failed to parse JSON from the cloud: " + e.getMessage());
            e.printStackTrace();
            throw new CloudException(e);
        }
        return address;
    }

    private @Nullable ResourceStatus toStatus(@Nullable JSONObject json, @Nullable JSONArray vmList, @Nullable JSONArray lbList) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        Boolean available = null;
        String id;

        try {
            if( json.has("id") && json.has("ip") ) {
                id = json.getString("id");
            }
            else {
                return null;
            }
            if( json.has("state") ) {
                JSONObject state = json.getJSONObject("state");

                if( state.has("id") ) {
                    int s = state.getInt("id");

                    if( s != 1 && (vmList == null || lbList == null) ) {
                        return null;
                    }
                    else if( s == 2 ) {
                        for( int i =0; i<vmList.length(); i++ ) {
                            JSONObject vm = vmList.getJSONObject(i);

                            if( !vm.has("id") ) {
                                continue;
                            }
                            if( vm.has("ip") ) {
                                JSONObject ip = vm.getJSONObject("ip");

                                if( ip.has("id") && ip.getString("id").equals(id) ) {
                                    available = false;
                                }
                            }
                        }
                        if( available == null ) {
                            for( int i=0; i<lbList.length(); i++ ) {
                                JSONObject lb = lbList.getJSONObject(i);

                                if( !lb.has("id") ) {
                                    continue;
                                }
                                if( lb.has("virtualip.ip") ) {
                                    JSONObject ip = lb.getJSONObject("virtualip.ip");


                                    if( ip.has("id") && ip.getString("id").equals(id) ) {
                                        available = false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return new ResourceStatus(id, available == null || available);
        }
        catch( JSONException e ) {
            logger.error("Failed to parse JSON from the cloud: " + e.getMessage());
            e.printStackTrace();
            throw new CloudException(e);
        }
    }
}
