/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Arrays;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Computes on-line estimates of mean, variance and all five quartiles (notably including the
 * median).  Since this is done in a completely incremental fashion (that is what is meant by
 * on-line) estimates are available at any time and the amount of memory used is constant.  Somewhat
 * surprisingly, the quantile estimates are about as good as you would get if you actually kept all
 * of the samples.
 * <p/>
 * The method used for mean and variance is Welford's method.  See
 * <p/>
 * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#On-line_algorithm
 * <p/>
 * The method used for computing the quartiles is a simplified form of the stochastic approximation
 * method described in the article "Incremental Quantile Estimation for Massive Tracking" by Chen,
 * Lambert and Pinheiro
 * <p/>
 * See
 * <p/>
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.105.1580
 */
@SuppressWarnings("serial")
public class StdDeviation extends BaseOperation<StdDeviation.Context> implements Aggregator<StdDeviation.Context> {

    public static final String FIELD_NAME = "stddeviation";

    /** Class Context is used to hold intermediate values. */
    protected static class Context {
        private boolean sorted = true;
        private boolean incremental = false;

        // the first several samples are kept so we can boot-strap our estimates
        // cleanly
        private double[] starter = new double[100];

        // quartile estimates
        private final double[] q = new double[5];

        // mean and variance estimates
        private double mean;
        private double variance;

        // number of samples seen so far
        private int count = 0;

        public Context reset() {
            count = 0;
            sorted = true;
            incremental = false;

            return this;
        }

        public void add(double sample) {
            sorted = false;

            count++;
            double oldMean = mean;
            mean += (sample - mean) / count;
            double diff = (sample - mean) * (sample - oldMean);
            variance += (diff - variance) / count;

            if (count < 100) {
                starter[count - 1] = sample;
            } else if (count == 100 && !incremental) {
                // when we first reach 100 elements, we switch to incremental
                // operation
                starter[count - 1] = sample;
                for (int i = 0; i <= 4; i++) {
                    q[i] = getQuartile(i);
                }
                // this signals any invocations of getQuartile at exactly 100
                // elements that we have
                // already switched to incremental operation
                incremental = true;
            } else {
                // n >= 100 && starter == null
                q[0] = Math.min(sample, q[0]);
                q[4] = Math.max(sample, q[4]);

                double rate = 2 * (q[3] - q[1]) / count;
                q[1] += (Math.signum(sample - q[1]) - 0.5) * rate;
                q[2] += Math.signum(sample - q[2]) * rate;
                q[3] += (Math.signum(sample - q[3]) + 0.5) * rate;

                if (q[1] < q[0]) {
                    q[1] = q[0];
                }

                if (q[3] > q[4]) {
                    q[3] = q[4];
                }
            }
        }

        private void sort() {
            if (!sorted && !incremental) {
                Arrays.sort(starter, 0, count);
                sorted = true;
            }
        }

        public double getSD() {
            return Math.sqrt(variance);
        }

        public double getQuartile(int i) {
            if (count > 100 || incremental) {
                return q[i];
            } else {
                sort();
                switch (i) {
                    case 0:
                    if (count == 0) {
                        throw new IllegalArgumentException("Must have at least one sample to estimate minimum value");
                    }
                    return starter[0];
                    case 1:
                    case 2:
                    case 3:
                    if (count >= 2) {
                        double x = i * (count - 1) / 4.0;
                        int k = (int) Math.floor(x);
                        double u = x - k;
                        return starter[k] * (1 - u) + starter[k + 1] * u;
                    } else {
                        throw new IllegalArgumentException("Must have at least two samples to estimate quartiles");
                    }
                    case 4:
                    if (count == 0) {
                        throw new IllegalArgumentException("Must have at least one sample to estimate maximum value");
                    }

                    return starter[count - 1];
                    default:
                    throw new IllegalArgumentException("Quartile number must be in the range [0..4] not " + i);
                }
            }
        }

    }

    /**
     * Constructs a new instance that returns the average of the values
     * encountered in the field name "average".
     */
    public StdDeviation() {
        super(1, new Fields(FIELD_NAME));
    }

    /**
     * Constructs a new instance that returns the average of the values
     * encountered in the given fieldDeclaration field name.
     * 
     * @param fieldDeclaration
     *            of type Fields
     */
    public StdDeviation(Fields fieldDeclaration) {
        super(1, fieldDeclaration);

        if (!fieldDeclaration.isSubstitution() && fieldDeclaration.size() != 1) {
            throw new IllegalArgumentException("fieldDeclaration may only declare 1 field, got: " + fieldDeclaration.size());
        }
    }

    public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        if (aggregatorCall.getContext() != null) {
            aggregatorCall.getContext().reset();
        } else {
            aggregatorCall.setContext(new Context());
        }
    }

    public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        Context context = aggregatorCall.getContext();
        TupleEntry arguments = aggregatorCall.getArguments();

        context.add(arguments.getDouble(0));
    }

    public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        aggregatorCall.getOutputCollector().add(getResult(aggregatorCall));
    }

    private Tuple getResult(AggregatorCall<Context> aggregatorCall) {
        Context context = aggregatorCall.getContext();

        return new Tuple(context.getSD());
    }
}
