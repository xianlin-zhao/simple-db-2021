package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private HashMap<Field, ArrayList<Integer>> aggList;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;
    private LinkedList<Integer> noGroupList;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;
        op = what;
        aggList = new HashMap<>();
        Type[] types;
        String[] strings;
        if (gbfield == NO_GROUPING) {
            noGroupList = new LinkedList<>();
            types = new Type[]{Type.INT_TYPE};
            strings = new String[]{"aggregateValue"};
        } else {
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
            strings = new String[]{"groupValue", "aggregateValue"};
        }
        tupleDesc = new TupleDesc(types, strings);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = (gbField != NO_GROUPING) ? tup.getField(gbField) : null;
        IntField intField = op != Op.COUNT ? (IntField) tup.getField(aField) : new IntField(0);
        if (gbField == NO_GROUPING) {
            noGroupList.add(intField.getValue());
        } else {
            if (!aggList.containsKey(field)) {
                aggList.put(field, new ArrayList<>());
            }
            aggList.get(field).add(intField.getValue());
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbField == NO_GROUPING) {
            Tuple noTuple = new Tuple(tupleDesc);
            int noResult = 0;
            int size = noGroupList.size();
            switch (op) {
                case MAX:
                    noResult = Integer.MIN_VALUE;
                    for (int i = 0; i < size; i++) {
                        noResult = Math.max(noResult, noGroupList.get(i));
                    }
                    break;
                case MIN:
                    noResult = Integer.MAX_VALUE;
                    for (int i = 0; i < size; i++) {
                        noResult = Math.min(noResult, noGroupList.get(i));
                    }
                    break;
                case COUNT:
                    noResult = size;
                    break;
                case SUM:
                    for (int i = 0; i < size; i++) {
                        noResult += noGroupList.get(i);
                    }
                    break;
                case AVG:
                    for (int i = 0; i < size; i++) {
                        noResult += noGroupList.get(i);
                    };
                    noResult = noResult / size;
                    break;
                default:
                    break;
            }
            noTuple.setField(0, new IntField(noResult));
            tuples.add(noTuple);
        } else {
            for (Field field : aggList.keySet()) {
                ArrayList<Integer> perList = aggList.get(field);
                Tuple perAggTuple = new Tuple(tupleDesc);
                perAggTuple.setField(0, field);
                int aggResult = 0;
                int listSize = perList.size();
                switch (op) {
                    case MAX:
                        aggResult = Integer.MIN_VALUE;
                        for (int i = 0; i < listSize; i++) {
                            aggResult = Math.max(aggResult, perList.get(i));
                        }
                        break;
                    case MIN:
                        aggResult = Integer.MAX_VALUE;
                        for (int i = 0; i < listSize; i++) {
                            aggResult = Math.min(aggResult, perList.get(i));
                        }
                        break;
                    case COUNT:
                        aggResult = listSize;
                        break;
                    case SUM:
                        for (int i = 0; i < listSize; i++) {
                            aggResult += perList.get(i);
                        }
                        break;
                    case AVG:
                        for (int i = 0; i < listSize; i++) {
                            aggResult += perList.get(i);
                        };
                        aggResult = aggResult / listSize;
                        break;
                    default:
                        break;
                }
                perAggTuple.setField(1, new IntField(aggResult));
                tuples.add(perAggTuple);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
