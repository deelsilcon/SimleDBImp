package simpledb.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author deelsilcon
 */
public class DependencyGraph {
    private long edgeCnt;
    private boolean hasCycle;
    private final Map<Long, List<Long>> adj;
    private final Map<Long, Boolean> marked;
    private final Map<Long, Boolean> onStack;


    public DependencyGraph() {
        this(0L);
    }

    public DependencyGraph(long edgeCnt) {
        this.adj = new ConcurrentHashMap<>();
        this.hasCycle = false;
        this.marked = new ConcurrentHashMap<>();
        this.onStack = new ConcurrentHashMap<>();
        this.edgeCnt = edgeCnt;
    }


    public long edges() {
        return this.edgeCnt;
    }

    public long vertices() {
        return this.adj.size();
    }

    public void addEdge(long v, long w) {
        adj.computeIfAbsent(w, k -> new ArrayList<>());
        adj.computeIfAbsent(v, k -> new ArrayList<>());
        adj.get(v).add(w);
        marked.put(v, false);
        onStack.put(w, false);
        edgeCnt++;
    }

    public List<Long> getAdjacent(long v) {
        return adj.get(v);
    }

    public DependencyGraph reverse() {
        DependencyGraph rev = new DependencyGraph();
        for (Long key : adj.keySet()) {
            for (Long val : getAdjacent(key)) {
                rev.addEdge(val, key);
            }
        }
        return rev;
    }

    public boolean checkCycle() {
        synchronized (this) {
            initState();
            for (Long key : adj.keySet()) {
                if (hasCycle) {
                    return true;
                }
                if (!marked.get(key)) {
                    dfs(key);
                }
            }
            return hasCycle;
        }
    }

    public void reset() {
        this.edgeCnt = 0;
        hasCycle = false;
        adj.clear();
        marked.clear();
        onStack.clear();
    }

    private void initState() {
        marked.replaceAll((k, v) -> false);
        hasCycle = false;
    }

    private void dfs(long v) {
        onStack.put(v, true);
        marked.put(v, true);
        if (getAdjacent(v) == null) {
            return;
        }
        for (Long w : getAdjacent(v)) {
            if (hasCycle) {
                return;
            } else if (!marked.get(w)) {
                dfs(w);
            } else if (onStack.get(w)) {
                hasCycle = true;
            }
        }
        onStack.put(v, false);
    }

    //use for debug
    public boolean hasCycle() {
        return hasCycle;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(vertices()).append(" vertices, ").append(edges()).append(" edges ").append("\n");
        for (long v : adj.keySet()) {
            s.append(String.format("%d: ", v));
            List<Long> a = getAdjacent(v);
            if (!a.isEmpty()) {
                for (long w : a) {
                    s.append(String.format("%d ", w));
                }
            } else {
                s.append("Null ");
            }
            s.append("\n");
        }
        return s.toString();
    }
}
