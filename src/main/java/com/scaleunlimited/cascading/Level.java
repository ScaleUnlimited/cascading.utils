package com.scaleunlimited.cascading;

/**
 * Log levels that are used along with the slf4j API
 * 
 *
 */
public enum Level {

    SLF4J_DEBUG ("DEBUG"),
    SLF4J_ERROR ("ERROR"),
    SLF4J_INFO ("INFO"),
    SLF4J_TRACE ("TRACE"),
    SLF4J_WARN ("WARN");
    
    private final String _level;

    private Level(String name) {
        _level = name;
    }
    
    @Override
    /**
     * Return the String name of this enum constant.
     * Note that for this enum toString will return a different result than the name() method.
     */
    public String toString() {
        return _level;
    }
}
