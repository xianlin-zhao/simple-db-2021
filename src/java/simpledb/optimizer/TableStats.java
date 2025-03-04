package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableId;
    private DbFile dbFile;
    private int IOPerPage;
    private int fieldNum;
    private TupleDesc td;
    private HashMap<Integer, IntHistogram> IntHists;
    private HashMap<Integer, StringHistogram> StringHists;
    private int pageNum;
    private int tupleNum;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        tableId = tableid;
        dbFile = Database.getCatalog().getDatabaseFile(tableid);
        IOPerPage = ioCostPerPage;
        td = dbFile.getTupleDesc();
        fieldNum = td.numFields();
        HeapFile heapFile = (HeapFile) dbFile;
        pageNum = heapFile.numPages();
        tupleNum = 0;
        IntHists = new HashMap<>();
        StringHists = new HashMap<>();
        DbFileIterator it = dbFile.iterator(null);
        int[] perMax = new int[fieldNum];
        int[] perMin = new int[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            perMax[i] = Integer.MIN_VALUE;
            perMin[i] = Integer.MAX_VALUE;
        }

        try {
            it.open();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            while (it.hasNext()) {
                Tuple tuple = it.next();
                tupleNum++;
                for (int i = 0; i < fieldNum; i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int nowValue = ((IntField) tuple.getField(i)).getValue();
                        perMax[i] = Math.max(perMax[i], nowValue);
                        perMin[i] = Math.min(perMin[i], nowValue);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < fieldNum; i++) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                IntHistogram hist1 = new IntHistogram(NUM_HIST_BINS, perMin[i], perMax[i]);
                IntHists.put(i, hist1);
            } else {
                StringHistogram hist2 = new StringHistogram(NUM_HIST_BINS);
                StringHists.put(i, hist2);
            }
        }

        try {
            it.rewind();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            while (it.hasNext()) {
                Tuple tuple = it.next();
                for (int i = 0; i < fieldNum; i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        int nowValue = ((IntField) tuple.getField(i)).getValue();
                        IntHistogram hist3 = IntHists.get(i);
                        hist3.addValue(nowValue);
                    } else {
                        String sValue = ((StringField) tuple.getField(i)).getValue();
                        StringHistogram hist4 = StringHists.get(i);
                        hist4.addValue(sValue);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        it.close();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return pageNum * IOPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (selectivityFactor * tupleNum);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (td.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram intHist = IntHists.get(field);
            return intHist.estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            StringHistogram stringHist = StringHists.get(field);
            return stringHist.estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleNum;
    }

}
