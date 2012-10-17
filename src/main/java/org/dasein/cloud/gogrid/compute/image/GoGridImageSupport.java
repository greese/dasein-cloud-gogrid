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

package org.dasein.cloud.gogrid.compute.image;

import org.apache.log4j.Logger;
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;

import org.dasein.cloud.gogrid.GoGrid;
import org.dasein.cloud.gogrid.GoGridMethod;
import org.dasein.cloud.identity.ServiceAction;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

/**
 * Support for GoGrid machine images.
 * <p>Created by George Reese: 10/14/12 8:35 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class GoGridImageSupport implements MachineImageSupport {
    static private final Logger logger = GoGrid.getLogger(GoGridImageSupport.class);

    private GoGrid provider;

    public GoGridImageSupport(GoGrid provider) { this.provider = provider; }

    @Override
    public void downloadImage(@Nonnull String machineImageId, @Nonnull OutputStream toOutput) throws CloudException, InternalException {
        throw new OperationNotSupportedException("You cannot currently download machine images from GoGrid");
    }

    private @Nonnull ProviderContext getContext() throws CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was provided for this request");
        }
        return ctx;
    }

    @Override
    public MachineImage getMachineImage(@Nonnull String machineImageId) throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.IMAGE_GET, new GoGridMethod.Param("image", machineImageId));

        if( list == null ) {
            return null;
        }

        for( int i=0; i<list.length(); i++ ) {
            try {
                MachineImage img = toImage(list.getJSONObject(i));

                if( img != null && img.getProviderMachineImageId().equals(machineImageId) ) {
                    return img;
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
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return "server image";
    }

    private @Nonnull String getRegionId(@Nonnull ProviderContext ctx) throws CloudException {
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was provided for this request");
        }
        return regionId;
    }

    @Override
    public boolean hasPublicLibrary() {
        return true;
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachine(final String vmId, final String name, final String description) throws CloudException, InternalException {
        final AsynchronousTask<String> task = new AsynchronousTask<String>();

        Thread t = new Thread() {
            public void run() {
                try {
                    task.completeWithResult(executeImaging(vmId, name, description));
                }
                catch( Throwable t ) {
                    task.complete(t);
                }
            }
        };

        t.setName("Imaging " + vmId + " as " + name);
        t.setDaemon(true);
        t.start();
        return task;
    }

    private String executeImaging(String vmId, String name,String description) throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);

        if( name == null ) {
            name = "Image of " + vmId;
        }
        if( description == null ) {
            description = name;
        }
        JSONArray list = method.get(GoGridMethod.IMAGE_SAVE, new GoGridMethod.Param("server", vmId), new GoGridMethod.Param("friendlyName", name), new GoGridMethod.Param("description", description));

        if( list == null ) {
            return null;
        }

        for( int i=0; i<list.length(); i++ ) {
            try {
                MachineImage img = toImage(list.getJSONObject(i));

                if( img != null ) {
                    return img.getProviderMachineImageId();
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
    public @Nonnull AsynchronousTask<String> imageVirtualMachineToStorage(String vmId, String name, String description, String directory) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No ability to image to storage");
    }

    @Override
    public @Nonnull String installImageFromUpload(@Nonnull MachineImageFormat format, @Nonnull InputStream imageStream) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No ability to install images from an existing file");
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.IMAGE_GET, new GoGridMethod.Param("image", machineImageId));

        if( list == null ) {
            return false;
        }

        for( int i=0; i<list.length(); i++ ) {
            try {
                JSONObject json = list.getJSONObject(i);

                if( json.has("isPublic") && json.has("id") && json.getString("id").equals(machineImageId) ) {
                    return json.getBoolean("isPublic");
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
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
    public @Nonnull Iterable<MachineImage> listMachineImages() throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        String regionId = getRegionId(ctx);

        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.IMAGE_LIST, new GoGridMethod.Param("datacenter", regionId));

        if( list == null ) {
            return Collections.emptyList();
        }
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();

        for( int i=0; i<list.length(); i++ ) {
            try {
                MachineImage img = toImage(list.getJSONObject(i));

                if( img != null && img.getProviderOwnerId() != null ) {
                    images.add(img);
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        return images;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(String accountId) throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        String regionId = getRegionId(ctx);

        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.IMAGE_LIST, new GoGridMethod.Param("datacenter", regionId));

        if( list == null ) {
            return Collections.emptyList();
        }
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();

        for( int i=0; i<list.length(); i++ ) {
            try {
                MachineImage img = toImage(list.getJSONObject(i));

                if( img != null && ((accountId == null && img.getProviderMachineImageId() != null) || (accountId != null && accountId.equals(img.getProviderOwnerId()))) ) {
                    images.add(img);
                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        return images;
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull String registerMachineImage(String atStorageLocation) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No registering machine images");
    }

    @Override
    public void remove(@Nonnull String machineImageId) throws CloudException, InternalException {
        GoGridMethod method = new GoGridMethod(provider);

        method.get(GoGridMethod.IMAGE_DELETE, new GoGridMethod.Param("id", machineImageId));
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        String regionId = getRegionId(ctx);

        GoGridMethod method = new GoGridMethod(provider);

        JSONArray list = method.get(GoGridMethod.IMAGE_LIST, new GoGridMethod.Param("datacenter", regionId));

        if( list == null ) {
            return Collections.emptyList();
        }
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();

        for( int i=0; i<list.length(); i++ ) {
            try {
                MachineImage img = toImage(list.getJSONObject(i));

                if( img != null ) {

                    if( keyword != null ) {
                        if( !img.getName().toLowerCase().contains(keyword) && !img.getDescription().toLowerCase().contains(keyword) ) {
                            continue;
                        }
                    }
                    if( platform != null && !platform.equals(Platform.UNKNOWN) ) {
                        Platform mine = img.getPlatform();

                        if( platform.isWindows() && !mine.isWindows() ) {
                            continue;
                        }
                        if( platform.isUnix() && !mine.isUnix() ) {
                            continue;
                        }
                        if( platform.isBsd() && !mine.isBsd() ) {
                            continue;
                        }
                        if( platform.isLinux() && !mine.isLinux() ) {
                            continue;
                        }
                        if( platform.equals(Platform.UNIX) ) {
                            if( !mine.isUnix() ) {
                                continue;
                            }
                        }
                        else if( !platform.equals(mine) ) {
                            continue;
                        }
                    }
                    if( architecture != null && !img.getArchitecture().equals(architecture) ) {
                        continue;
                    }
                    images.add(img);

                }
            }
            catch( JSONException e ) {
                logger.error("Failed to parse JSON: " + e.getMessage());
                e.printStackTrace();
                throw new CloudException(e);
            }
        }
        return images;
    }

    @Override
    public void shareMachineImage(@Nonnull String machineImageId, @Nullable String withAccountId, boolean allow) throws CloudException, InternalException {
        if( withAccountId != null ) {
            throw new OperationNotSupportedException("Cannot share images with specific accounts");
        }

        GoGridMethod method = new GoGridMethod(provider);

        method.get(GoGridMethod.IMAGE_EDIT, new GoGridMethod.Param("id", machineImageId), new GoGridMethod.Param("isPublic", allow ? "true" : "false"));
    }

    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsImageSharing() {
        return false;
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
        return true;
    }

    @Override
    public @Nonnull String transfer(@Nonnull CloudProvider fromCloud, @Nonnull String machineImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No transfering of machine images");
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable MachineImage toImage(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        MachineImage img = new MachineImage();
        String regionId = getRegionId(getContext());

        img.setPlatform(Platform.UNKNOWN);
        img.setType(MachineImageType.STORAGE);
        img.setCurrentState(MachineImageState.PENDING);
        img.setSoftware("");
        img.setProviderRegionId(regionId);
        try {
            if( json.has("id") ) {
                img.setProviderMachineImageId(json.getString("id"));
            }
            if( json.has("owner") ) {
                JSONObject owner = json.getJSONObject("owner");

                if( owner != null && owner.has("id") ) {
                    long id = owner.getLong("id");

                    if( id > 0L ) {
                        img.setProviderOwnerId(String.valueOf(id));
                    }
                }
            }
            /*
            if( json.has("createdTime") ) {
                long ts = json.getLong("createdTime") * 1000L;
                // TODO: implement this when Dasein supports it
            }
            */
            if( json.has("os") ) {
                JSONObject os = json.getJSONObject("os");
                StringBuilder str = new StringBuilder();

                if( os.has("name") ) {
                    str.append(os.getString("name"));
                }
                if( os.has("description") ) {
                    str.append(" ").append(os.getString("description"));
                }
                img.setPlatform(Platform.guess(str.toString()));
                img.setArchitecture(provider.toArchitecture(str.toString()));
            }
            if( json.has("state") ) {
                JSONObject state = json.getJSONObject("state");

                if( state.has("id") ) {
                    int id = state.getInt("id");

                    /*
                   [{"id":1,"description":"Image is being saved","name":"Saving","object":"option"},
                   {"id":2,"description":"Image is available for adds","name":"Available","object":"option"},
                   {"id":3,"description":"Image has been deleted","name":"Deleted","object":"option"},
                   {"id":4,"description":"Image is marked for deletion","name":"Trash","object":"option"},
                   {"id":7,"description":"Image is being migrated from another data center","name":"Migrating","object":"option"}]
                    */
                    switch( id ) {
                        case 1: case 7: img.setCurrentState(MachineImageState.PENDING); break;
                        case 2: img.setCurrentState(MachineImageState.ACTIVE); break;
                        case 3: case 4: img.setCurrentState(MachineImageState.DELETED); break;
                    }
                }
            }
            if( json.has("architecture") ) {
                JSONObject architecture = json.getJSONObject("architecture");

                if( architecture.has("id") && architecture.getInt("id") == 1 ) {
                    img.setArchitecture(Architecture.I32);
                }
                else if( architecture.has("id") && architecture.getInt("id") == 2 ) {
                    img.setArchitecture(Architecture.I64);
                }
            }
            if( json.has("datacenterlist") ) {
                JSONArray list = json.getJSONArray("datacenterlist");
                boolean matches = false;

                for( int i=0; i<list.length(); i++ ) {
                    JSONObject item = list.getJSONObject(i);

                    if( item.has("datacenter") ) {
                        item = item.getJSONObject("datacenter");
                        if( item.has("id") && item.getString("id").equals(regionId) ) {
                            matches = true;
                            break;
                        }
                    }
                }
                if( !matches ) {
                    return null;
                }
            }
        }
        catch( JSONException e ) {
            logger.error("Failed to process image JSON: " + e.getMessage());
            e.printStackTrace();
            throw new CloudException(e);
        }
        if( img.getProviderMachineImageId() == null ) {
            return null;
        }
        if( img.getName() == null ) {
            img.setName(img.getProviderMachineImageId());
        }
        if( img.getDescription() == null ) {
            img.setDescription(img.getName());
        }
        return img;
    }
}
