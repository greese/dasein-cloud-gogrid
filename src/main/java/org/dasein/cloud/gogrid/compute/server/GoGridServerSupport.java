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

package org.dasein.cloud.gogrid.compute.server;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.AbstractVMSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.gogrid.GoGrid;
import org.dasein.cloud.gogrid.GoGridMethod;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.AddressType;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddress;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Implements interaction with the GoGrid server APIs.
 * <p>Created by George Reese: 10/13/12 8:16 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class GoGridServerSupport extends AbstractVMSupport {
    static private final Logger logger = GoGrid.getLogger(GoGridServerSupport.class);

    private GoGrid provider;

    public GoGridServerSupport(GoGrid provider) {
        super(provider);
        this.provider = provider;
    }

    private @Nonnull String getRegionId(@Nonnull ProviderContext ctx) throws CloudException {
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was provided for this request");
        }
        return regionId;
    }

    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "server";
    }

    @Override
    public VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.SERVER_GET, new GoGridMethod.Param("server", vmId));

        if( list == null ) {
            return null;
        }

        for( int i=0; i<list.length(); i++ ) {
            try {
                VirtualMachine vm = toServer(list.getJSONObject(i));

                if( vm != null && vm.getProviderVirtualMachineId().equals(vmId) ) {
                    return vm;
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
    public @Nonnull Requirement identifyImageRequirement(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return (cls.equals(ImageClass.MACHINE) ? Requirement.REQUIRED : Requirement.NONE);
    }

    @Override
    public @Nonnull Requirement identifyPasswordRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyRootVolumeRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyStaticIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyVlanRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isBasicAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isExtendedAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);
        String regionId = getRegionId(getContext());

        JSONArray regionList = method.get(GoGridMethod.LOOKUP_LIST, new GoGridMethod.Param("lookup", "server.datacenter"));

        if( regionList == null ) {
            return false;
        }
        for( int i=0; i<regionList.length(); i++ ) {
            try {
                JSONObject r = regionList.getJSONObject(i);

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
    public boolean isUserDataSupported() throws CloudException, InternalException {
        return false;
    }

    static private final Random random = new Random();

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        GoGridMethod.Param[] params = new GoGridMethod.Param[5];
        String name = validateName(withLaunchOptions.getHostName());

        params[0] = new GoGridMethod.Param("name", name);
        params[1] = new GoGridMethod.Param("image", withLaunchOptions.getMachineImageId());
        params[2] = new GoGridMethod.Param("server.ram", withLaunchOptions.getStandardProductId());
        //params[3] = new GoGridMethod.Param("datacenter", getRegionId(getContext()));
        params[3] = new GoGridMethod.Param("description", withLaunchOptions.getDescription());

        IpAddress target = null;

        for( IpAddress address : provider.getNetworkServices().getIpAddressSupport().listIpPool(IPVersion.IPV4, true) ) {
            if( address.getAddressType().equals(AddressType.PUBLIC) ) {
                target = address;
                if( random.nextInt(10) == 4 ) { // try to avoid conflict between two simultaneous calls
                    break;
                }
            }
            else if( target == null ) {
                target = address;
            }
        }
        if( target == null ) {
            logger.error("Could not identify an available IP address for launch");
            throw new CloudException("Unable to identify an available IP address");
        }
        if( logger.isDebugEnabled() ) {
            logger.debug("IP address for launch: " + target.getAddress());
        }
        if( target.getAddressType().equals(AddressType.PRIVATE) ) {
            params[4] = new GoGridMethod.Param("privateip", target.getProviderIpAddressId());
        }
        else {
            params[4] = new GoGridMethod.Param("ip", target.getProviderIpAddressId());
        }

        GoGridMethod method = new GoGridMethod(provider);

        if( logger.isDebugEnabled() ) {
            logger.debug("Launching VM: " + withLaunchOptions.getHostName());
        }
        JSONArray launches = method.get(GoGridMethod.SERVER_ADD, params);
        VirtualMachine vm = null;

        if( logger.isDebugEnabled() ) {
            logger.debug("launch list=" + launches);
            if( launches != null ) {
                logger.debug("size=" + launches.length());
            }
        }
        name = null;
        if( launches != null && launches.length() == 1 ) {
            try {
                JSONObject json = launches.getJSONObject(0);

                if( json.has("name") ) {
                    name = json.getString("name");
                }
                vm = toServer(json);
            }
            catch( JSONException e ) {
                logger.error("Launches did not come back in the form of a valid list: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        if( vm == null ) {
            if( name == null ) {
                name = withLaunchOptions.getHostName();
            }
            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 15L);

            while( System.currentTimeMillis() < timeout ) {
                try { Thread.sleep(45000L); }
                catch( InterruptedException ignore ) { }
                for( VirtualMachine s : listVirtualMachines() ) {
                    if( s.getName().equalsIgnoreCase(name) ) {
                        if( logger.isDebugEnabled() ) {
                            logger.debug("server=" + s);
                        }
                        return s;
                    }
                }
            }
            throw new CloudException("System timed out waiting for VM ID");
        }
        if( logger.isDebugEnabled() ) {
            logger.debug("server=" + vm);
        }
        return vm;
    }

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    static private HashMap<String,Map<Architecture,Collection<VirtualMachineProduct>>> productCache;

    @Override
    public @Nonnull Iterable<VirtualMachineProduct> listProducts(@Nonnull Architecture architecture) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No region was set for this request");
        }
        if( productCache != null ) {
            Map<Architecture,Collection<VirtualMachineProduct>> cached = productCache.get(ctx.getEndpoint());

            if( cached != null ) {
                Collection<VirtualMachineProduct> c = cached.get(architecture);

                if( c == null ) {
                    return Collections.emptyList();
                }
                return c;
            }
        }
        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.LOOKUP_LIST, new GoGridMethod.Param("lookup", "server.ram"));

        if( list == null ) {
            return Collections.emptyList();
        }

        ArrayList<VirtualMachineProduct> products = new ArrayList<VirtualMachineProduct>();

        for( int i=0; i<list.length(); i++ ) {
            try {
                VirtualMachineProduct prd = toProduct(list.getJSONObject(i));

                if( prd != null ) {
                    products.add(prd);
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        HashMap<Architecture,Collection<VirtualMachineProduct>> map = new HashMap<Architecture, Collection<VirtualMachineProduct>>();

        map.put(Architecture.I32, Collections.unmodifiableList(products));
        map.put(Architecture.I64, Collections.unmodifiableList(products));
        if( productCache == null ) {
            HashMap<String,Map<Architecture,Collection<VirtualMachineProduct>>> pm = new HashMap<String, Map<Architecture, Collection<VirtualMachineProduct>>>();

            pm.put(ctx.getEndpoint(), map);
            productCache = pm;
        }
        return products;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        ProviderContext ctx = getContext();
        String regionId = getRegionId(ctx);

        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.SERVER_LIST, new GoGridMethod.Param("datacenter", regionId));

        if( list == null ) {
            return Collections.emptyList();
        }
        ArrayList<ResourceStatus> servers = new ArrayList<ResourceStatus>();

        for( int i=0; i<list.length(); i++ ) {
            try {
                ResourceStatus vm = toStatus(list.getJSONObject(i));

                if( vm != null ) {
                    servers.add(vm);
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        return servers;
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        ProviderContext ctx = getContext();
        String regionId = getRegionId(ctx);

        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.SERVER_LIST, new GoGridMethod.Param("datacenter", regionId));

        if( list == null ) {
            return Collections.emptyList();
        }
        ArrayList<VirtualMachine> servers = new ArrayList<VirtualMachine>();

        for( int i=0; i<list.length(); i++ ) {
            try {
                VirtualMachine vm = toServer(list.getJSONObject(i));

                if( vm != null ) {
                    servers.add(vm);
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        return servers;
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);

        method.get(GoGridMethod.SERVER_POWER, new GoGridMethod.Param("id", vmId), new GoGridMethod.Param("power", "restart"));
    }

    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        GoGridMethod method = new GoGridMethod(provider);

        method.get(GoGridMethod.SERVER_POWER, new GoGridMethod.Param("id", vmId), new GoGridMethod.Param("power", "start"));
    }

    @Override
    public void stop(@Nonnull String vmId, /* ignored */ boolean force) throws InternalException, CloudException {
        GoGridMethod method = new GoGridMethod(provider);

        method.get(GoGridMethod.SERVER_POWER, new GoGridMethod.Param("id", vmId), new GoGridMethod.Param("power", "stop"));
    }

    @Override
    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        return true;
    }

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
        GoGridMethod method = new GoGridMethod(provider);

        method.get(GoGridMethod.SERVER_DELETE, new GoGridMethod.Param("id", vmId));
        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 15L);

        while( timeout > System.currentTimeMillis() ) {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null || vm.getCurrentState().equals(VmState.TERMINATED) ) {
                return;
            }
            try { Thread.sleep(15000L); }
            catch( InterruptedException ignore ) { }
        }
    }

    @Override
    public void terminate(@Nonnull String vmId, @Nullable String explanation) throws InternalException, CloudException {
        terminate(vmId);
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable VirtualMachineProduct toProduct(@Nullable JSONObject json) throws CloudException, InternalException {
        //{"summary":{"total":7,"start":0,"numpages":0,"returned":7},"status":"success","method":"/common/lookup/list",
        // "list":[{"id":1,"description":"Server with 512MB RAM","name":"512MB","object":"option"},{"id":2,"description":"Server with 1GB RAM","name":"1GB","object":"option"},{"id":3,"description":"Server with 2GB RAM","name":"2GB","object":"option"},{"id":4,"description":"Server with 4GB RAM","name":"4GB","object":"option"},{"id":5,"description":"Server with 8GB RAM","name":"8GB","object":"option"},{"id":8,"description":"Server with 16GB RAM","name":"16GB","object":"option"},{"id":9,"description":"Server with 24GB RAM","name":"24GB","object":"option"}]}
        if( json == null ) {
            return null;
        }

        VirtualMachineProduct product = new VirtualMachineProduct();

        product.setRootVolumeSize(new Storage<Gigabyte>(10, Storage.GIGABYTE));
        try {
            if( json.has("id") ) {
                product.setProviderProductId(json.getString("id"));
            }
            if( json.has("name") ) {
                product.setName(json.getString("name"));
                try {
                    product.setRamSize(Storage.valueOf(json.getString("name")));
                }
                catch( Throwable ignore ) {
                    product.setRamSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
                }
            }
            if( json.has("description") ) {
                product.setDescription(json.getString("description"));
            }
            product.setCpuCount(1);
        }
        catch( JSONException e ) {
            logger.error("Invalid JSON from cloud: " + e.getMessage());
            e.printStackTrace();
            throw new CloudException(e);
        }
        if( product.getProviderProductId() == null ) {
            return null;
        }
        if( product.getName() == null ) {
            product.setName(product.getProviderProductId());
        }
        if( product.getDescription() == null ) {
            product.setDescription(product.getName());
        }
        return product;
    }

    /*
    [{"id":1,"description":"Server is in active state.","name":"On","object":"option"},
    {"id":2,"description":"Server is in transient state...Starting.","name":"Starting","object":"option"},
    {"id":3,"description":"Server is in inactive state.","name":"Off","object":"option"},
    {"id":4,"description":"Server is in transient state...Stopping.","name":"Stopping","object":"option"},
    {"id":5,"description":"Server is in transient state...Restarting","name":"Restarting","object":"option"},
    {"id":6,"description":"Server is in transient state...Saving","name":"Saving","object":"option"},
    {"id":7,"description":"Server is in transient state...Restoring","name":"Restoring","object":"option"},
    {"id":8,"description":"Server is in transient state...Updating","name":"Updating","object":"option"},
    {"id":9,"description":"Server is in a transient state but is on...Saving","name":"On/Saving","object":"option"},
    {"id":10,"description":"Server is in a transient state but is off...Saving","name":"Off/Saving","object":"option"}]
     */
    private @Nonnull VmState toState(int id) {
        switch( id ) {
            case 1: return VmState.RUNNING;
            case 2: return VmState.PENDING;
            case 3: return VmState.STOPPED;
            case 4: return VmState.STOPPING;
            case 5: return VmState.REBOOTING;
            case 6: return VmState.PENDING;
            case 7: return VmState.PENDING;
            case 8: return VmState.PENDING;
            case 9: return VmState.RUNNING;
            case 10: return VmState.STOPPED;
        }
        logger.warn("DEBUG: Unknown virtual machine state: " + id);
        return VmState.PENDING;
    }

    private @Nullable VirtualMachine toServer(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        VirtualMachine vm = new VirtualMachine();
        String regionId = getRegionId(getContext());

        vm.setCurrentState(VmState.PENDING);
        vm.setProviderOwnerId(getContext().getAccountNumber());
        vm.setProviderRegionId(regionId);
        vm.setProviderSubnetId(null);
        vm.setProviderVlanId(null);
        vm.setImagable(false);
        vm.setProviderDataCenterId(vm.getProviderRegionId() + "a");
        vm.setPlatform(Platform.UNKNOWN);
        vm.setArchitecture(Architecture.I64);
        vm.setPersistent(true);
        vm.setRebootable(false);
        try {
            if( json.has("id") ) {
                vm.setProviderVirtualMachineId(json.getString("id"));
            }
            if( json.has("name") ) {
                vm.setName(json.getString("name"));
            }
            if( json.has("description") ) {
                vm.setDescription(json.getString("description"));
            }
            if( json.has("datacenter") ) {
                JSONObject dc = json.getJSONObject("datacenter");

                if( dc.has("id") ) {
                    vm.setProviderRegionId(dc.getString("id"));
                    vm.setProviderDataCenterId(vm.getProviderRegionId() + "a");
                    if( !regionId.equals(vm.getProviderRegionId()) ) {
                        return null;
                    }
                }
            }
            if( json.has("state") ) {
                JSONObject state = json.getJSONObject("state");

                if( state.has("id") ) {
                    vm.setCurrentState(toState(state.getInt("id")));
                }
                if( vm.getCurrentState().equals(VmState.RUNNING) ) {
                    vm.setRebootable(true);
                }
            }
            if( json.has("os") ) {
                JSONObject os = json.getJSONObject("os");
                StringBuilder str = new StringBuilder();

                if( os.has("name") ) {
                    str.append(os.getString("name"));
                }
                if( os.has("description") ) {
                    str.append(" ").append(os.getString("description"));
                }
                vm.setPlatform(Platform.guess(str.toString()));
                vm.setArchitecture(provider.toArchitecture(str.toString()));
            }
            if( json.has("image") ) {
                JSONObject image = json.getJSONObject("image");

                if( image.has("id") ) {
                    vm.setProviderMachineImageId(image.getString("id"));
                }
                if( image.has("architecture") ) {
                    JSONObject architecture = image.getJSONObject("architecture");

                    if( architecture.has("id") ) {
                        int id = architecture.getInt("id");

                        if( id == 1 ) {
                            vm.setArchitecture(Architecture.I32);
                        }
                        else {
                            vm.setArchitecture(Architecture.I64);
                        }
                    }
                }
            }
            boolean hasPriv = false;

            if( json.has("ip") ) {
                JSONObject ip = json.getJSONObject("ip");
                boolean pub = (ip.has("public") && ip.getBoolean("public"));

                if( ip.has("id") ) {
                    vm.setProviderAssignedIpAddressId(ip.getString("id"));
                }
                if( ip.has("ip") ) {
                    String addr = ip.getString("ip");

                    if( addr != null ) {
                        if( pub ) {
                            vm.setPublicIpAddresses(new String[] { addr });
                        }
                        else {
                            hasPriv = true;
                            vm.setPrivateIpAddresses(new String[] { addr });
                        }
                    }
                }
            }
            if( !hasPriv && json.has("privateip") ) {
                JSONObject ip = json.getJSONObject("privateip");

                if( ip.has("ip") ) {
                    String addr = ip.getString("ip");

                    if( addr != null ) {
                        vm.setPrivateIpAddresses(new String[] { addr });
                    }
                }
            }
            if( json.has("isSandbox") ) {
                vm.setImagable(json.getBoolean("isSandbox"));
            }
            if( json.has("ram") ) {
                JSONObject product = json.getJSONObject("ram");

                if( product.has("id") ) {
                    vm.setProductId(product.getString("id"));
                }
            }
        }
        catch( JSONException e ) {
            logger.error("Failed to parse JSON from the cloud: " + e.getMessage());
            e.printStackTrace();
            throw new CloudException(e);
        }
        if( vm.getProviderVirtualMachineId() == null ) {
            logger.warn("Object had no ID: " + json);
            return null;
        }
        if( vm.getName() == null ) {
            vm.setName(vm.getProviderVirtualMachineId());
        }
        if( vm.getDescription() == null ) {
            vm.setDescription(vm.getName());
        }
        vm.setClonable(false);
        vm.setImagable(vm.getCurrentState().equals(VmState.STOPPED));
        return vm;
    }

    private @Nullable ResourceStatus toStatus(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        try {
            String id;

            if( json.has("id") ) {
                id = json.getString("id");
            }
            else {
                return null;
            }
            if( json.has("state") ) {
                JSONObject state = json.getJSONObject("state");

                if( state.has("id") ) {
                    return new ResourceStatus(id, toState(state.getInt("id")));
                }
            }
            return new ResourceStatus(id, VmState.PENDING);
        }
        catch( JSONException e ) {
            logger.error("Failed to parse JSON from the cloud: " + e.getMessage());
            e.printStackTrace();
            throw new CloudException(e);
        }
    }

    private boolean exists(@Nonnull String name, @Nonnull Iterable<VirtualMachine> vms) {
        for( VirtualMachine vm : vms ) {
            if( vm.getName().equalsIgnoreCase(name) ) {
                return true;
            }
        }
        return false;
    }

    private @Nonnull String validateName(@Nonnull String name) throws CloudException, InternalException {
        Iterable<VirtualMachine> current = listVirtualMachines();

        if( name.length() < 21 && !exists(name, current) ) {
            return name;
        }
        boolean available = true;
        String base = name;

        if( base.length() > 17 ) {
            base = base.substring(0, 17);
        }
        for( int idx=1; idx<1000; idx++ ) {
            String tmp = base + idx;

            if( !exists(tmp, current) ) {
                return tmp;
            }
        }
        throw new CloudException("Invalid virtual machine name: " + name);
    }
}
