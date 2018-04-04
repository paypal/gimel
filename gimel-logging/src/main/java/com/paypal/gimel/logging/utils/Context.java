/*
 * Copyright 2018 PayPal Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.paypal.gimel.logging.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;


/**
 * Contains information which can be reused.
 * <p>
 * Note: {@code Context} is a singleton instance.
 * </p>
 *
 */
public class Context {

    private String host;
    private String ipAddress;

    // Sccas metrics details
    private String profile;
    private String tenant;
    private String appServiceName;
    private Map<String, String> dimensions;
    private int resolution;

    public Context() {
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getLocalHost();
            this.host = inetAddress.getHostName();
            this.ipAddress = inetAddress.getHostAddress();
            this.dimensions = new HashMap<String, String>();
        }
        catch (final UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * @return the profile
     */
    public String getProfile() {
        return this.profile;
    }

    /**
     * @param profileName
     *            the profile to set
     */
    public void setProfileName(final String profileName) {
        this.profile = profileName;
    }

    /**
     * @return the appServiceName
     */
    public String getAppServiceName() {
        return this.appServiceName;
    }

    /**
     * @return the dimensions
     */
    public Map<String, String> getDimensions() {
        return this.dimensions;
    }

    /**
     * @param appServiceName
     *            the appServiceName to set
     */
    public void setAppServiceName(final String appServiceName) {
        this.appServiceName = appServiceName;
    }

    /**
     * @return the tenant
     */
    public String getTenant() {
        return this.tenant;
    }

    /**
     * @param tenant
     *            the tenant to set
     */
    public void setTenant(final String tenant) {
        this.tenant = tenant;
    }

    /**
     * @return the ipAddress
     */
    public String getIpAddress() {
        return this.ipAddress;
    }

    /**
     * @param ipAddress
     *            the ipAddress to set
     */
    public void setIpAddress(final String ipAddress) {
        this.ipAddress = ipAddress;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return this.host;
    }

    /**
     * @return the resolution
     */
    public int getResolution() {
        return resolution;
    }

    /**
     * Set the hostname
     *
     * @param host
     *            the host to set
     */
    public void setHost(final String host) {
        this.host = host;
    }

}
