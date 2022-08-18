package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private DbFile dbFile;
    private List<Tuple> affected;
    private Iterator<Tuple> it;
    private TupleDesc td;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableId);
        if (!child.getTupleDesc().equals(dbFile.getTupleDesc())) {
            throw new DbException("TupleDesc of child differs from table.");
        }
        td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"inserted number"});
        Tuple result = new Tuple(td);
        affected = new ArrayList<>();
        affected.add(result);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        int insertNum = 0;
        child.open();
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
            insertNum++;
        }
        affected.get(0).setField(0, new IntField(insertNum));
        it = affected.iterator();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = affected.iterator();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }
}
