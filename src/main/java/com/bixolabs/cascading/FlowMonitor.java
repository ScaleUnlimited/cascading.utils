package com.bixolabs.cascading;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import cascading.flow.FlowStep;
import cascading.flow.StepCounters;
import cascading.stats.FlowStats;
import cascading.stats.StepStats;
import cascading.stats.CascadingStats.Status;

public class FlowMonitor {

    private static final Logger LOGGER = Logger.getLogger(FlowMonitor.class);

    public static final String FILENAME = "flow-monitor.html";
    public static final int DEFAULT_UPDATE_INTERVAL = 10000;
    public static final int DEFAULT_ROWS_PER_STEP = 10;
    
    private static final String MONITOR_TOP_HTML = "/monitor-top.html";
    private static final String MONITOR_STEP_HTML = "/monitor-step.html";
    private static final String MONITOR_ROW_HTML = "/monitor-row.html";

    private static final String DEFAULT_HADOOP_LOG_DIR = "/mnt/hadoop/logs/";
    private static final String DEFAULT_LOCAL_LOG_DIR = "./";
    
    private Flow _flow;
    private Throwable _flowException;
    private int _updateInterval;
    private boolean _includeCascadingCounters;
    private File _htmlDir;
    private List<IMonitorTask> _tasks;
    private int _timeEntriesPerStep;
    private List<StepEntry> _stepEntries;
    
    private String _htmlTopTemplate;
    private String _htmlStepTemplate;
    private String _htmlRowTemplate;
    
    public FlowMonitor(Flow flow) throws IOException {
        _flow = flow;
        
        _updateInterval = DEFAULT_UPDATE_INTERVAL;
        _timeEntriesPerStep = DEFAULT_ROWS_PER_STEP;
        _includeCascadingCounters = false;
        _htmlDir = null;
        
        _tasks = new ArrayList<IMonitorTask>();
        
        _htmlTopTemplate = IOUtils.toString(FlowMonitor.class.getResourceAsStream(MONITOR_TOP_HTML));
        _htmlStepTemplate = IOUtils.toString(FlowMonitor.class.getResourceAsStream(MONITOR_STEP_HTML));
        _htmlRowTemplate = IOUtils.toString(FlowMonitor.class.getResourceAsStream(MONITOR_ROW_HTML));
        
        _stepEntries = new ArrayList<StepEntry>();
        for (FlowStep step : _flow.getSteps()) {
            _stepEntries.add(new StepEntry(step));
        }
    }
    
    public Flow getFlow() {
        return _flow;
    }

    public File getHtmlDirectory() {
        return _htmlDir;
    }

    public void setHtmlDirectory(String dir) {
        _htmlDir = new File(dir);
    }
    
    public int getRowsPerStep() {
        return _timeEntriesPerStep;
    }

    public void setRowsPerStep(int rowsPerStep) {
        _timeEntriesPerStep = rowsPerStep;
    }

    public int getUpdateInterval() {
        return _updateInterval;
    }

    public void setUpdateInterval(int updateInterval) {
        _updateInterval = updateInterval;
    }
    
    public void setIncludeCascadingCounters(boolean includeCascadingCounters) {
        _includeCascadingCounters = includeCascadingCounters;
    }
    
    public boolean isIncludeCascadingCounters() {
        return _includeCascadingCounters;
    }

    public void addMonitorTask(IMonitorTask task) {
        _tasks.add(task);
    }
    
    @SuppressWarnings("unchecked")
    public boolean run(Enum... counters) throws Throwable {
        if (_htmlDir == null) {
            _htmlDir = getDefaultLogDir(_flow.getJobConf());
        }
        
        _flowException = null;
        FlowListener catchExceptions = new FlowListener() {

            @Override
            public void onCompleted(Flow flow) { }

            @Override
            public void onStarting(Flow flow) { }

            @Override
            public void onStopping(Flow flow) { }

            @Override
            public boolean onThrowable(Flow flow, Throwable t) {
                _flowException = t;
                return true;
            }
        };
        
        _flow.addListener(catchExceptions);
        _flow.start();
        
        FlowStats stats;
        
        do {
            Thread.sleep(_updateInterval);

            stats = _flow.getFlowStats();
            List<StepStats> stepStats = stats.getStepStats();

            for (StepStats stepStat : stepStats) {
                int stepId = (Integer)stepStat.getID();
                StepEntry stepEntry = findStepById(stepId);
                Status oldStatus = stepEntry.getStatus();
                Status newStatus = stepStat.getStatus();
                if (oldStatus != newStatus) {
                    stepEntry.setStartTime(stepStat.getStartTime());
                    stepEntry.setStatus(stepStat.getStatus());
                }
                
                if ((oldStatus == Status.RUNNING) || (newStatus == Status.RUNNING)) {
                    if (stepStat.isFinished()) {
                        stepEntry.setDuration(stepStat.getDuration());
                    } else if (stepStat.isRunning()) {
                        stepEntry.setDuration(System.currentTimeMillis() - stepEntry.getStartTime());
                    } else {
                        // Duration isn't known
                        stepEntry.setDuration(0);
                    }
                    
                    stepEntry.addTimeEntry(makeTimeEntry(stepEntry, stepStat, counters), _timeEntriesPerStep);
                }
            }

            // Now we can build our resulting table
            StringBuilder topTemplate = new StringBuilder(_htmlTopTemplate);
            replace(topTemplate, "%flowname%", StringEscapeUtils.escapeHtml(_flow.getName()));

            for (StepEntry stepEntry : _stepEntries) {
                StringBuilder stepTemplate = new StringBuilder(_htmlStepTemplate);
                replaceHtml(stepTemplate, "%stepname%", stepEntry.getName());
                replaceHtml(stepTemplate, "%stepstatus%", "" + stepEntry.getStatus());
                replaceHtml(stepTemplate, "%stepstart%", new Date(stepEntry.getStartTime()).toString());
                replaceHtml(stepTemplate, "%stepduration%", "" + (stepEntry.getDuration() / 1000));
                
                replace(stepTemplate, "%counternames%", getTableHeader(stepEntry.getStep(), counters));
                
                // Now we need to build rows of data, for steps that are running or have finished.
                if (stepEntry.getStatus() != Status.PENDING) {
                    for (TimeEntry row : stepEntry.getTimerEntries()) {
                        StringBuilder rowTemplate = new StringBuilder(_htmlRowTemplate);
                        replaceHtml(rowTemplate, "%timeoffset%", "" + (row.getTimeDelta() / 1000));
                        replace(rowTemplate, "%countervalues%", getCounterValues(row.getCounterValues()));
                        insert(stepTemplate, "%steprows%", rowTemplate.toString());
                    }
                }
                
                // Get rid of position marker we used during inserts.
                replace(stepTemplate, "%steprows%", "");
                
                insert(topTemplate, "%steps%", stepTemplate.toString());
            }
            
            // Get rid of position marker we used during inserts.
            replace(topTemplate, "%steps%", "");
            
            // We've got the template ready to go, create the file.
            File htmlFile = new File(_htmlDir, FILENAME);
            FileWriterWithEncoding fw = new FileWriterWithEncoding(htmlFile, "UTF-8");
            IOUtils.write(topTemplate.toString(), fw);
            fw.close();
        } while (!stats.isFinished());

        // Create a copy of the file as an archive, once we're done.
        File htmlFile = new File(_htmlDir, FILENAME);
        File archiveFile = new File(_htmlDir, String.format("%s-%s", _flow.getName(), FILENAME));
        archiveFile.delete();
        
        if (!htmlFile.exists() || archiveFile.exists()) {
            LOGGER.warn("Unable to create archive of file " + htmlFile.getAbsolutePath());
        } else {
            try {
                String content = IOUtils.toString(new FileReader(htmlFile));
                FileWriterWithEncoding fw = new FileWriterWithEncoding(archiveFile, "UTF-8");
                IOUtils.write(content, fw);
                fw.close();
            } catch (Exception e) {
                LOGGER.warn("Unable to create archive of file " + htmlFile.getAbsolutePath(), e);
            }
        }
        
        if (stats.isFailed() && (_flowException != null)) {
            throw _flowException;
        }
        
        return stats.isSuccessful();
    }
    
    private StepEntry findStepById(int stepId) {
        for (StepEntry stepEntry : _stepEntries) {
            if (stepEntry.getId() == stepId) {
                return stepEntry;
            }
        }

        throw new RuntimeException("Can't find StepEntry with id " + stepId);
    }

    @SuppressWarnings("unchecked")
    private TimeEntry makeTimeEntry(StepEntry stepEntry, StepStats stepStats, Enum... counters) {
        FlowStep flowStep = stepEntry.getStep();
        TimeEntry result = new TimeEntry(stepEntry.getDuration());
        for (Enum counter : counters) {
            result.addCounterValue("" + StepUtils.safeGetCounter(stepStats, counter));
        }

        for (IMonitorTask task : _tasks) {
            try {
                result.addCounterValue(task.getValue(_flow, flowStep, stepStats));
            } catch (Throwable t) {
                LOGGER.error("Exception thrown by MonitorTask!", t);
                result.addCounterValue("<error>");
            }
        }
        
        if (_includeCascadingCounters) {
            for (Enum counter : StepCounters.values()) {
                result.addCounterValue("" + StepUtils.safeGetCounter(stepStats, counter));
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private String getTableHeader(FlowStep flowStep, Enum...counters) {        
        StringBuilder header = new StringBuilder();
        for (Enum counter : counters) {
            header.append("<td>");
            header.append(StringEscapeUtils.escapeHtml(counter.toString()));
            header.append("</td>");
        }

        for (IMonitorTask task : _tasks) {
            header.append("<td>");
            header.append(StringEscapeUtils.escapeHtml(task.getName(_flow, flowStep)));
            header.append("</td>");
        }
        
        if (_includeCascadingCounters) {
            for (Enum counter : StepCounters.values()) {
                header.append("<td>");
                header.append(StringEscapeUtils.escapeHtml(counter.toString()));
                header.append("</td>");
            }
        }

        return header.toString();
    }
    
    private String getCounterValues(List<String> counterValues) {
        StringBuilder result = new StringBuilder();
        for (String value : counterValues) {
            result.append("<td>");
            result.append(StringEscapeUtils.escapeHtml(value));
            result.append("</td>");
        }

        return result.toString();
    }


    private void replace(StringBuilder template, String key, String value) {
        int offset = template.indexOf(key);
        if (offset == -1) {
            throw new RuntimeException("Key doesn't exist in template: " + key);
        }
        
        template.delete(offset, offset + key.length());
        template.insert(offset, value);
    }

    private void replaceHtml(StringBuilder template, String key, String value) {
        replace(template, key, StringEscapeUtils.escapeHtml(value));
    }

    private void insert(StringBuilder template, String key, String value) {
        int offset = template.indexOf(key);
        if (offset == -1) {
            throw new RuntimeException("Key doesn't exist in template: " + key);
        }
        
        template.insert(offset, value);
    }

    private File getDefaultLogDir(JobConf conf) {
        File result;
        
        if (isJobLocal(conf)) {
            result = new File(DEFAULT_LOCAL_LOG_DIR);
            if (!result.exists()) {
                result.mkdir();
            }
        } else {
            String hadoopLogDir = System.getProperty("HADOOP_LOG_DIR");
            if (hadoopLogDir == null) {
                hadoopLogDir = System.getProperty("hadoop.log.dir");
            }
            
            if (hadoopLogDir == null) {
                String hadoopHomeDir = System.getProperty("HADOOP_HOME");
                if (hadoopHomeDir != null) {
                    hadoopLogDir = hadoopHomeDir = "/logs";
                }
            }
            
            if (hadoopLogDir == null) {
                hadoopLogDir = DEFAULT_HADOOP_LOG_DIR;
            }
            
            LOGGER.info("Setting monitor output directory to: " + hadoopLogDir);
            result = new File(hadoopLogDir);
        }

        if (!result.exists() || !result.isDirectory()) {
            throw new RuntimeException("Can't find default location for HTML file: " + result);
        }
        
        return result;
    }
    
    private static boolean isJobLocal(JobConf conf) {
        return conf.get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
    }

    private static class TimeEntry {
        private long _timeDelta;
        private List<String> _counterValues;
        
        public TimeEntry(long timeDelta) {
            _timeDelta = timeDelta;
            _counterValues = new ArrayList<String>();
        }

        @SuppressWarnings("unused")
        public TimeEntry(long timeDelta, List<String> counterValues) {
            _timeDelta = timeDelta;
            _counterValues = counterValues;
        }

        public long getTimeDelta() {
            return _timeDelta;
        }

        public List<String> getCounterValues() {
            return _counterValues;
        }
        
        public void addCounterValue(String counterValue) {
            _counterValues.add(counterValue);
        }
    }
    
    private static class StepEntry {
        Status _status;
        FlowStep _step;
        @SuppressWarnings("unused")
        StepStats _stats;
        long _startTime;
        long _duration;
        List<TimeEntry> _timeEntries;
        
        public StepEntry(FlowStep step) {
            _step = step;
            _status = Status.PENDING;
            
            _timeEntries = new ArrayList<TimeEntry>();
        }
        
        public FlowStep getStep() {
            return _step;
        }
        
        public Status getStatus() {
            return _status;
        }
        
        public void setStatus(Status status) {
            _status = status;
        }
        
        public String getName() {
            return _step.getStepName();
        }
        
        public int getId() {
            return _step.getID();
        }
        
        public long getStartTime() {
            return _startTime;
        }

        public void setStartTime(long startTime) {
            _startTime = startTime;
        }

        public long getDuration() {
            return _duration;
        }

        public void setDuration(long duration) {
            _duration = duration;
        }

        public List<TimeEntry> getTimerEntries() {
            return _timeEntries;
        }
        
        public void addTimeEntry(TimeEntry timeEntry, int maxTimeEntries) {
            _timeEntries.add(timeEntry);
            while (_timeEntries.size() > maxTimeEntries) {
                _timeEntries.remove(0);
            }
        }
        
    }
    

}
