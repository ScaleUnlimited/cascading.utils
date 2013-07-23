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

package com.scaleunlimited.cascading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingFlowReporter implements IFlowReporter {
    private static Logger LOGGER = LoggerFactory.getLogger(LoggingFlowReporter.class);

    @Override
    public void setStatus(Level level, String msg) {
        switch(level) {
            case SLF4J_DEBUG : LOGGER.debug(msg); break;
            case SLF4J_ERROR : LOGGER.error(msg); break;
            case SLF4J_INFO : LOGGER.info(msg); break;
            case SLF4J_TRACE : LOGGER.trace(msg); break;
            case SLF4J_WARN : LOGGER.warn(msg); break;
        }
    }

    @Override
    public void setStatus(String msg, Throwable t) {
        LOGGER.error(msg, t);
    }
}
