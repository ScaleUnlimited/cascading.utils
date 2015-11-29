package com.scaleunlimited.cascading;

import java.util.Set;

import org.apache.commons.lang.ArrayUtils;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.operation.Aggregator;
import cascading.operation.Buffer;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.ConfigDef;
import cascading.tuple.Fields;

public class PipeHelper {

    private Pipe _pipe;
    
    public PipeHelper(String name) {
        _pipe = new Pipe(name);
    }
    
    public PipeHelper(Pipe pipe) {
        _pipe = pipe;
    }
    
    public PipeHelper(String name, Pipe previous) {
        _pipe = new Pipe(name, previous);
    }
    
    public PipeHelper(String name, PipeHelper previous) {
        _pipe = new Pipe(name, previous.getPipe());
    }
    
    public Pipe getPipe() {
        return _pipe;
    }
    
    // ===========================================================================================
    // Below here are all of the methods that provide a wrapper over the pipe
    // ===========================================================================================

    // Fields we use when building up state for an operation
    private Fields _groupFields;
    private Fields _sortFields;
    private boolean _reverseSort = false;
    
    // TODO add some way to determine if reverseSort has been set, and
    // fail if used without a sortWith() call.
    
    public PipeHelper groupWith(String... groupFieldnames) {
        return groupWith(new Fields(groupFieldnames));
    }
    
    public PipeHelper groupWith(Fields groupFields) {
        _groupFields = groupFields;
        return this;
    }
    
    public PipeHelper sortWith(String... sortFieldnames) {
        return sortWith(new Fields(sortFieldnames));
    }
    
    public PipeHelper sortWith(Fields sortFields) {
        _sortFields = sortFields;
        return this;
    }
    
    public PipeHelper reverseSort() {
        _reverseSort = true;
        return this;
    }
    
    public PipeHelper group() {
        return group();
    }
    
    public PipeHelper group(PipeHelper... pipes) {
        Pipe[] incomingPipes = new Pipe[pipes.length + 1];
        incomingPipes[0] = _pipe;
        for (int i = 0; i < pipes.length; i++) {
            incomingPipes[i + 1] = pipes[i].getParent();
        }
        
        if (_sortFields != null) {
            _pipe = new GroupBy(incomingPipes, _groupFields, _sortFields, _reverseSort);
        } else {
            _pipe = new GroupBy(incomingPipes, _groupFields);
        }

        clearGroupSettings();
        return this;
    }
    
    public PipeHelper topN(int topN) {
        if (_sortFields == null) {
            throw new IllegalArgumentException("You must set sort fields before calling top");
        }
        
        if (_groupFields == null) {
            _groupFields = Fields.NONE;
        }
        
        _pipe = new GroupBy(_pipe, _groupFields, _sortFields, _reverseSort);
        _pipe = new Every(_pipe, new First(topN));
        
        clearGroupSettings();
        return this;
    }
    
    /**
     * Clear out settings so we don't accidentally pick them up later.
     */
    private void clearGroupSettings() {
        _groupFields = null;
        _sortFields = null;
        _reverseSort = false;
    }

    // Synonyms for groupWith(), feels better when doing a join to call these.
    // But sets the _groupFields, same as groupWith.
    
    public PipeHelper joinWith(String... joinFieldnames) {
        return groupWith(joinFieldnames);
    }
    
    public PipeHelper joinWith(Fields joinFields) {
        return groupWith(joinFields);
    }
    

    public static PipeHelper leftJoin(PipeHelper lhs, PipeHelper... rhs) {
        // all pipe helpers must have groupFields set.
        // none should have sortFields or reverseSet set.
        // if all rhs are "small" use HashJoin if it's not an outer join???
        
        return null;
    }
    
    // TODO add join support, where it's always joining multiple pipes to this one?
    // Or we always return a new PipeHelper, so it's static...probably more logical.
    // Each PipeHelper is called to get the joining fields.
    // Decide if we do auto-rename for collisions with group fieldnames? We'd need
    // to get rid of the temp renamed values later. But differs if it's a left vs.
    // right join re which one is the "main" one, and outer join has both as equally
    // important and needing to maintain, so we can't handle that here.
    
    // Hmm, could have have an expected size (or "is small" setting) for a pipe, and
    // automatically use HashJoin?
    
    // Have leftJoin, rightJoin, innerJoin, outerJoin calls?
    // Use each PipeHelper's groupFields to 
    

//    // Grouping functions that return a new PipeHelper using the incoming pipes.
//    public PipeHelper hashJoin(String name, Pipe lhs, String leftJoin, Pipe rhs, String rightJoin) {
//        _pipe = new HashJoin(name, lhs, leftJoin, rhs, rightJoin, new LeftJoin());
//        return this;
//    }
//
//    public PipeHelper hashJoin(String name, Pipe lhs, String leftJoin, Pipe rhs, String rightJoin, Joiner joiner) {
//        return new PipeHelper(new HashJoin(name, lhs, new Fields(leftJoin), rhs, new Fields(rightJoin), joiner));
//    }
//
//    public PipeHelper coGroup(String name, Pipe lhs, String leftJoin, Pipe rhs, String rightJoin) {
//        return CoGroup(name, lhs, leftJoin, rhs, rightJoin, new LeftJoin());
//    }
//
//    public PipeHelper coGroup(String name, Pipe lhs, String leftJoin, Pipe rhs, String rightJoin, Joiner joiner) {
//        return new PipeHelper(new CoGroup(name, lhs, new Fields(leftJoin), rhs, new Fields(rightJoin), joiner));
//    }
//
    // These are all of the methods for operations on the current pipe
    public PipeHelper discard(String... fieldnames) {
        return discard(new Fields(fieldnames));
    }

    public PipeHelper discard(Fields fields) {
        _pipe = new Discard(_pipe, fields);
        return this;
    }

    public PipeHelper retain(String... fieldnames) {
        return retain(new Fields(fieldnames));
    }

    public PipeHelper retain(Fields fields) {
        _pipe = new Retain(_pipe, fields);
        return this;
    }

    public PipeHelper rename(String from, String to) {
        _pipe = new Rename(_pipe, new Fields(from), new Fields(to));
        return this;
    }

    public PipeHelper debug() {
        return debug(_pipe.getName());
    }

    public PipeHelper debug(String name) {
        return debug(name, false);
    }

    public PipeHelper debug(String name, boolean displayFields) {
        _pipe = new Each(_pipe, DebugLevel.VERBOSE, new Debug(name, displayFields));
        return this;
    }

    public PipeHelper debugShowFields() {
        return debug(_pipe.getName(), true);
    }

    public PipeHelper debugShowFields(String name) {
        return debug(name, true);
    }

    public PipeHelper filter(Filter<?> filter, String... fields) {
        _pipe = new Each(_pipe, new Fields(fields), filter);
        return this;
    }

    public PipeHelper filterString(String expression, String... fields) {
        _pipe = new Each(_pipe, new Fields(fields), new ExpressionFilter(expression, String.class));
        return this;
    }

    public PipeHelper filterBool(String expression, String... fields) {
        _pipe = new Each(_pipe, new Fields(fields), new ExpressionFilter(expression, Boolean.class));
        return this;
    }

    public PipeHelper filterLong(String expression, String... fields) {
        _pipe = new Each(_pipe, new Fields(fields), new ExpressionFilter(expression, Long.class));
        return this;
    }

    public PipeHelper filterDouble(String expression, String... fields) {
        _pipe = new Each(_pipe, new Fields(fields), new ExpressionFilter(expression, Double.class));
        return this;
    }

    public PipeHelper filterFloat(String expression, String... fields) {
        _pipe = new Each(_pipe, new Fields(fields), new ExpressionFilter(expression, Float.class));
        return this;
    }

    public PipeHelper filterRegex(String regex, boolean removeMatch, boolean matchEachElement, String... fields) {
        _pipe = new Each(_pipe, new Fields(fields), new RegexFilter(regex, removeMatch, matchEachElement));
        return this;
    }

    public PipeHelper function(Function<?> function, Fields outputSelector) {
        _pipe = new Each(_pipe, function, outputSelector);
        return this;
    }

    public PipeHelper function(Function<?> function, Fields outputSelector, String... fields) {
        _pipe = new Each(_pipe, new Fields(fields), function, outputSelector);
        return this;
    }

    public PipeHelper every(Aggregator<?> aggregator, Fields outputSelector) {
        _pipe = new Every(_pipe, aggregator, outputSelector);
        return this;
    }

    public PipeHelper every(Aggregator<?> aggregator, Fields outputSelector, String... fields) {
        _pipe = new Every(_pipe, new Fields(fields), aggregator, outputSelector);
        return this;
    }

    public PipeHelper every(Buffer<?> buffer, Fields outputSelector) {
        _pipe = new Every(_pipe, buffer, outputSelector);
        return this;
    }

    public PipeHelper every(Buffer<?> buffer, Fields outputSelector, String... fields) {
        _pipe = new Every(_pipe, new Fields(fields), buffer, outputSelector);
        return this;
    }
    
    // ===========================================================================================
    // Below here are all of the delegate methods for the Pipe that we wrap
    // ===========================================================================================

    /**
     * @return
     * @see cascading.pipe.Pipe#getName()
     */
    public String getName() {
        return _pipe.getName();
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#getPrevious()
     */
    public Pipe[] getPrevious() {
        return _pipe.getPrevious();
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#getParent()
     */
    public Pipe getParent() {
        return _pipe.getParent();
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#getConfigDef()
     */
    public ConfigDef getConfigDef() {
        return _pipe.getConfigDef();
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#hasConfigDef()
     */
    public boolean hasConfigDef() {
        return _pipe.hasConfigDef();
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#getStepConfigDef()
     */
    public ConfigDef getStepConfigDef() {
        return _pipe.getStepConfigDef();
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#hasStepConfigDef()
     */
    public boolean hasStepConfigDef() {
        return _pipe.hasStepConfigDef();
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#getHeads()
     */
    public Pipe[] getHeads() {
        return _pipe.getHeads();
    }

    /**
     * @param incomingScopes
     * @return
     * @see cascading.pipe.Pipe#outgoingScopeFor(java.util.Set)
     */
    public Scope outgoingScopeFor(Set<Scope> incomingScopes) {
        return _pipe.outgoingScopeFor(incomingScopes);
    }

    /**
     * @param incomingScope
     * @return
     * @see cascading.pipe.Pipe#resolveIncomingOperationArgumentFields(cascading.flow.planner.Scope)
     */
    public Fields resolveIncomingOperationArgumentFields(Scope incomingScope) {
        return _pipe.resolveIncomingOperationArgumentFields(incomingScope);
    }

    /**
     * @param incomingScope
     * @return
     * @see cascading.pipe.Pipe#resolveIncomingOperationPassThroughFields(cascading.flow.planner.Scope)
     */
    public Fields resolveIncomingOperationPassThroughFields(Scope incomingScope) {
        return _pipe.resolveIncomingOperationPassThroughFields(incomingScope);
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#getTrace()
     */
    public String getTrace() {
        return _pipe.getTrace();
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#toString()
     */
    public String toString() {
        return _pipe.toString();
    }

    /**
     * @param element
     * @return
     * @see cascading.pipe.Pipe#isEquivalentTo(cascading.flow.FlowElement)
     */
    public boolean isEquivalentTo(FlowElement element) {
        return _pipe.isEquivalentTo(element);
    }

    /**
     * @param object
     * @return
     * @see cascading.pipe.Pipe#equals(java.lang.Object)
     */
    public boolean equals(Object object) {
        return _pipe.equals(object);
    }

    /**
     * @return
     * @see cascading.pipe.Pipe#hashCode()
     */
    public int hashCode() {
        return _pipe.hashCode();
    }

    /**
     * @param scope
     * @return
     * @see cascading.pipe.Pipe#print(cascading.flow.planner.Scope)
     */
    public String print(Scope scope) {
        return _pipe.print(scope);
    }


}
