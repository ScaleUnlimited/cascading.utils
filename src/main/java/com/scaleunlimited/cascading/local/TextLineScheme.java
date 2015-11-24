package com.scaleunlimited.cascading.local;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.local.TextLine;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class TextLineScheme extends TextLine {

    private boolean _compress = false;
    
    public TextLineScheme() {
        this(false);
    }

    public TextLineScheme(boolean compress) {
        super();
        
        _compress = compress;
    }

    public TextLineScheme(Fields sourceFields) {
        super(sourceFields);
    }

    public TextLineScheme(Fields sourceFields, Fields sinkFields) {
        this(sourceFields, sinkFields, false);
    }

    public TextLineScheme(Fields sourceFields, Fields sinkFields, boolean compress) {
        super(sourceFields, sinkFields);
        
        _compress = compress;
    }

    @Override
    public LineNumberReader createInput(InputStream inputStream) {
        if (!inputStream.markSupported()) {
            inputStream = new BufferedInputStream(inputStream, 128);
        }

        // See if inputStream is gzipped.
        inputStream.mark(2);
        byte[] magic = new byte[2];

        try {
            int bytesRead = inputStream.read(magic);
            inputStream.reset();

            if ((bytesRead == 2) && (magic[0] == (byte)0x1f) && (magic[1] == (byte)0x8b)) {
                return super.createInput(new GZIPInputStream(inputStream));
            }
        } catch (IOException e) {
            // Ignore, and just return regular reader
        }

        return super.createInput(inputStream);
    }
    
    @Override
    public PrintWriter createOutput(OutputStream outputStream) {
        if (_compress) {
            try {
                outputStream = new GZIPOutputStream(outputStream);
            } catch (IOException e) {
                // Ignore the error, and we'll return a regular PrintWriter.
            }
        }
        
        return super.createOutput(outputStream);
    }
    
    @Override
    public void sinkCleanup(FlowProcess<? extends Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall) throws IOException {
        // Currently cascading doesn't close the printwriter, so the underlying stream doesn't get flushed properly.
        PrintWriter pw = sinkCall.getContext();
        super.sinkCleanup(flowProcess, sinkCall);
        
        try {
            pw.close();
        } catch (Exception e) {
            // Just in case Cascading starts closing it.
        }
    }
}
