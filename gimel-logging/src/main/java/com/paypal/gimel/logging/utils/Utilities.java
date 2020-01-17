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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class Utilities {

    private final static Logger LOGGER = LogManager.getLogger(Utilities.class);

    /**
     * Get system hostname
     *
     * @return hostname if available otherwise returns empty string
     */
    public static String getHostname() {
        InetAddress ip;
        String hostname = "";
        try {
            ip = InetAddress.getLocalHost();
            hostname = ip.getHostName();
        } catch (UnknownHostException e) {
            // Ignore the exception and keep marching
            LOGGER.error("Unable to fetch local hostname" + e.getMessage());
        }
        return hostname;
    }

    public static boolean isReachable(String hostname, int port) {

        try (Socket socket = new Socket()) {
            SocketAddress socketAddress = new InetSocketAddress(hostname, port);
            int timeout = 100;
            socket.connect(socketAddress, timeout);
            return true;
        } catch (IOException ex) {
            // Ignore and march
            LOGGER.warn(String.format("Host %s is not listening on %d", hostname, port));
        }

        return false;
    }

}