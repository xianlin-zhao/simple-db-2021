package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private HashMap<Field, Integer> aggResult;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;
    private int noGroupResult;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;
        op = what;
        aggResult = new HashMap<>();
        Type[] types;
        String[] strings;
        if (gbfield == NO_GROUPING) {
            noGroupResult = 0;
            types = new Type[]{Type.INT_TYPE};
            strings = new String[]{"aggregateValue"};
        } else {
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
            strings = new String[]{"groupValue", "aggregateValue"};
        }
        tupleDesc = new TupleDesc(types, strings);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = (gbField != NO_GROUPING) ? tup.getField(gbField) : null;
        if (gbField == NO_GROUPING) {
            noGroupResult += 1;
        } else {
            if (!aggResult.containsKey(field)) {
                aggResult.put(field, 0);
            }
            int oriCount = aggResult.get(field);
            aggResult.put(field, oriCount + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbField == NO_GROUPING) {
            Tuple noTuple = new Tuple(tupleDesc);
            noTuple.setField(0, new IntField(noGroupResult));
            tuples.add(noTuple);
        } else {
            for (Field field : aggResult.keySet()) {
                int perResult = aggResult.get(field);
                Tuple perAggTuple = new Tuple(tupleDesc);
                perAggTuple.setField(0, field);
                perAggTuple.setField(1, new IntField(perResult));
                tuples.add(perAggTuple);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
