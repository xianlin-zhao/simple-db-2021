package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    public static class PageLock {
        public PageId pid;
        public Permissions perm;
        public int holdNum;

        public PageLock(PageId pid, Permissions perm) {
            this.pid = pid;
            this.perm = perm;
            this.holdNum = 1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PageLock pageLock = (PageLock) o;
            return pid.equals(pageLock.pid);
        }

        @Override
        public int hashCode() {
            return pid.hashCode();
        }

    }

    public static class WaitGraph {
        private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> graph;

        public WaitGraph() {
            graph = new ConcurrentHashMap<>();
        }

        public void addVertex(TransactionId point) {
            if (graph.get(point) != null) {
                return;
            }
            graph.put(point, new HashSet<>());
        }

        public void addEdge(TransactionId src, TransactionId dst) {
            addVertex(src);
            addVertex(dst);
            HashSet<TransactionId> toPoints = graph.get(src);
            if (!toPoints.contains(dst)) {
                toPoints.add(dst);
            }
        }

        public void removeEdge(TransactionId src, TransactionId dst) {
            if (graph.get(src) != null && graph.get(dst) != null && graph.get(src).contains(dst)) {
                graph.get(src).remove(dst);
            }
        }

        public void removeVertex(TransactionId point) {
            if (graph.get(point) != null) {
                for (TransactionId tid : graph.keySet()) {
                    if (graph.get(tid).contains(point)) {
                        graph.get(tid).remove(point);
                    }
                }
                graph.remove(point);
            }
        }

        public boolean haveCycle() {
            ConcurrentHashMap<TransactionId, Integer> visit = new ConcurrentHashMap<>();
            for (TransactionId tid : graph.keySet()) {
                visit.put(tid, 0);
            }
            for (TransactionId tid : graph.keySet()) {
                if (visit.get(tid) == 0) {
                    if (dfs(tid, visit)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean dfs(TransactionId tid, ConcurrentHashMap<TransactionId, Integer> visit) {
            visit.put(tid, 1);
            HashSet<TransactionId> toVertexes = graph.get(tid);
            for (TransactionId dst : toVertexes) {
                if (visit.get(dst) == 0) {
                    if (dfs(dst, visit)) {
                        return true;
                    }
                } else if (visit.get(dst) == 1) {
                    return true;
                }
            }
            visit.put(tid, 2);
            return false;
        }
    }

    public ConcurrentHashMap<PageId, Set<TransactionId>> pageTransIds;
    public ConcurrentHashMap<TransactionId, Set<PageLock>> transLocks;
    public WaitGraph waitGraph;
    public ConcurrentHashMap<PageId, Boolean> writerWaiting;

    public LockManager() {
        pageTransIds = new ConcurrentHashMap<>();
        transLocks = new ConcurrentHashMap<>();
        waitGraph = new WaitGraph();
        writerWaiting = new ConcurrentHashMap<>();
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
        Set<TransactionId> transOnPage = pageTransIds.get(pid);
        Set<PageLock> locksForTrans = transLocks.get(tid);
        if (transOnPage == null || transOnPage.size() == 0) {
            PageLock lock = new PageLock(pid, perm);
            HashSet<TransactionId> transSet = new HashSet<>();
            transSet.add(tid);
            pageTransIds.put(pid, transSet);
            if (locksForTrans == null) {
                HashSet<PageLock> lockSet = new HashSet<>();
                lockSet.add(lock);
                transLocks.put(tid, lockSet);
            } else {
                locksForTrans.add(lock);
            }
            return;
        }

        PageLock lock = findLockForPage(pid);
        if (perm == Permissions.READ_ONLY) {
            if (locksForTrans != null && locksForTrans.contains(new PageLock(pid, Permissions.READ_ONLY))) {
                return;
            }
            if (writerWaiting.get(pid) != null && writerWaiting.get(pid) == true) {
                throw new TransactionAbortedException();
            }
            if (lock.perm == Permissions.READ_WRITE) {
                addToWaitGraph(tid, transOnPage);
                if (waitGraph.haveCycle()) {
                    removeFromWaitGraph(tid, transOnPage);
                    throw new TransactionAbortedException();
                }
                while (lock.holdNum != 0) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                waitGraph.removeVertex(tid);
            }
        } else {
            boolean flagReading = false;
            if (transOnPage.contains(tid)) {
                if (transOnPage.size() == 1) {
                    lock.perm = Permissions.READ_WRITE;
                    return;
                } else {
                    flagReading = true;
                }
            }
            addToWaitGraph(tid, transOnPage);
            if (waitGraph.haveCycle()) {
                removeFromWaitGraph(tid, transOnPage);
                throw new TransactionAbortedException();
            }
            writerWaiting.put(pid, true);
            while (!(lock.holdNum == 0 || (lock.holdNum == 1 && flagReading))) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            writerWaiting.put(pid, false);
            waitGraph.removeVertex(tid);
        }

        lock.perm = perm;
        lock.holdNum++;
        if (locksForTrans == null || locksForTrans.size() == 0) {
            HashSet<PageLock> lockSet = new HashSet<>();
            lockSet.add(lock);
            transLocks.put(tid, lockSet);
        } else {
            locksForTrans.add(lock);
        }
        transOnPage.add(tid);
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (holdLock(tid, pid)) {
            Set<TransactionId> transOnPage = pageTransIds.get(pid);
            Set<PageLock> locksForTrans = transLocks.get(tid);
            PageLock lock = findLockForPage(pid);
            lock.holdNum--;
            transOnPage.remove(tid);
            locksForTrans.remove(lock);
            this.notifyAll();
        }
    }

    public synchronized boolean holdLock(TransactionId tid, PageId pid) {
        return pageTransIds.get(pid) != null && pageTransIds.get(pid).contains(tid);
    }

    private synchronized PageLock findLockForPage(PageId pid) {
        Set<TransactionId> transOnPage = pageTransIds.get(pid);
        Iterator<TransactionId> it = transOnPage.iterator();
        Set<PageLock> tidLocks = transLocks.get(it.next());
        PageLock ansLock = null;
        for (PageLock lock : tidLocks) {
            if (lock.equals(new PageLock(pid, Permissions.READ_ONLY))) {
                ansLock = lock;
                break;
            }
        }
        return ansLock;
    }

    private synchronized void addToWaitGraph(TransactionId tid, Set<TransactionId> transOnPage) {
        for (TransactionId t : transOnPage) {
            if (!tid.equals(t)) {
                waitGraph.addEdge(tid, t);
            }
        }
    }

    private synchronized void removeFromWaitGraph(TransactionId tid, Set<TransactionId> transOnPage) {
        for (TransactionId t : transOnPage) {
            waitGraph.removeEdge(tid, t);
        }
        waitGraph.removeVertex(tid);
    }

    public synchronized HashSet<PageLock> getTransLocks(TransactionId tid) {
        return new HashSet<>(transLocks.getOrDefault(tid, Collections.emptySet()));
    }
}
