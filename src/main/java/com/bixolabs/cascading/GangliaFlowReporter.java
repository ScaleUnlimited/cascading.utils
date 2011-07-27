/**
 * Copyright 2010 TransPac Software, Inc.
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
 */

package com.bixolabs.cascading;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GangliaFlowReporter implements IFlowReporter {
    private static final Logger LOGGER = Logger.getLogger(GangliaFlowReporter.class);

    private static final int GANGLIA_DEFAULT_PORT = 8649;
    private final static int GANGLIA_SLOPE_UNSPECIFIED = 4;
    private final static String GANGLIA_VALUE_STRING = "string";

    private InetAddress _address;
    private int _port;
    
    public GangliaFlowReporter(){
       String serverPort = getDefaultServerPort();
       if (serverPort != null ) {
           String server = null;
            int index = serverPort.indexOf(":");
            if (index != -1) { // we assume the string is of the type name:port
                String portStr = serverPort.substring(index+1);
                _port = Integer.parseInt(portStr);
            } else {
                server = serverPort;
                _port = GANGLIA_DEFAULT_PORT;
            }
            try {
                _address =  InetAddress.getByName(server);
            } catch (UnknownHostException e) {
                LOGGER.info("Unable to resolve server name: " + server);
                _address = null;
            }
       }
    }
    
    public GangliaFlowReporter(InetAddress address, int port) {
        _address = address;
        _port = port;
    }
    
    @Override
    public void setStatus(Level level, String msg) {
        if (_address != null) {
            sendToGanglia(_address, _port, level.toString(), msg, GANGLIA_VALUE_STRING, "", GANGLIA_SLOPE_UNSPECIFIED, 100, 100);
        }
    }

    @Override
    public void setStatus(String msg, Throwable t) {
        if (_address != null){
            sendToGanglia(_address, _port, "Throwable", msg, GANGLIA_VALUE_STRING, "", GANGLIA_SLOPE_UNSPECIFIED, 100, 100);
        }
    }

    @SuppressWarnings("deprecation")
    private boolean isLocalJob() {
        JobConf jobConf = new JobConf();
        return jobConf.get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
    }
    
    private String getDefaultServerPort() {
        String serverPort = null;

        try {
            ContextFactory contextFactory = ContextFactory.getFactory();
            serverPort = (String)contextFactory.getAttribute("mapred.servers");
        } catch (IOException e) {
            if (isLocalJob()) {
                // Only log an error when not running in local mode
                LOGGER.error("Unable to get context factory to determine ganglia server", e);
            }
        }

        return serverPort;
    }
    
    private static void sendToGanglia(InetAddress address, int port, String name, String value, String type, String units, int slope, int tmax, int dmax) {
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();
            byte[] buf = write(name, value, type, units, slope, tmax, dmax);
            DatagramPacket p = new DatagramPacket(buf, buf.length, address, port);
            socket.send(p);
        } catch (IOException e) {
            LOGGER.debug("Unable to send data to Ganglia", e);
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }
    
    private static byte[] write(String name, String value, String type, String units, int slope, int tmax, int dmax) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeInt(0);
            writeXDRString(dos, type);
            writeXDRString(dos, name);
            writeXDRString(dos, value);
            writeXDRString(dos, units);
            dos.writeInt(slope);
            dos.writeInt(tmax);
            dos.writeInt(dmax);
            return baos.toByteArray();
        } catch (IOException e) {
            // really this is impossible
            return null;
        }
    }

    private static void writeXDRString(DataOutputStream dos, String s) throws IOException {
        dos.writeInt(s.length());
        dos.writeBytes(s);
        int offset = s.length() % 4;
        if (offset != 0) {
            for (int i = offset; i < 4; ++i) {
                dos.writeByte(0);
            }
        }
    }

}


