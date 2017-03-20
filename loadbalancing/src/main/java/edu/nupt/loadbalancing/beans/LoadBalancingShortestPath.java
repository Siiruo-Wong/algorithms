/*
 * @Copyright (c) 2017 Nanjing University Of Posts And Telecommunications (NUPT).  All rights reserved.
 */
package edu.nupt.loadbalancing.beans;

import java.util.Collection;
import java.util.HashMap;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import edu.uci.ics.jung.algorithms.shortestpath.ShortestPath;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseMultigraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import edu.uci.ics.jung.graph.util.Pair;

/**
 * 
 * @Project: loadbalancing
 * @Description: The class is the sub-class of the shortest path algorithm
 *               {@link edu.nupt.loadbalancing.beans.LoadBalancingDistance<V,
 *               E>} based on the load balancing strategy, overrides and creates
 *               some methods for Load-Balancing, and also implements interface
 *               {@link edu.uci.ics.jung.algorithms.shortestpath.ShortestPath<V,E>}
 *               to facilitate the invocation of other modules in OpenDayLight
 *               {@link https://www.opendaylight.org/}.
 * @Author: Yanjun Wang
 * @Date: 2017年3月17日
 */
public class LoadBalancingShortestPath<V, E> extends LoadBalancingDistance<V, E> implements ShortestPath<V, E> {
	protected List<NuptPath> results;
	protected List<NuptPath> candidates;
	private boolean more = true;

	/**
	 * Creates a LoadBalancingShortestPath instance based on a given global
	 * network graph {# g:{@link edu.uci.ics.jung.graph.Graph<V, E>}} and the
	 * parameter k {# k:the order of K-Top shortest path algorithm}.
	 * 
	 * @param g
	 *            a graph of global network
	 * @param k
	 *            the order of K-Top shortest algorithm
	 */
	public LoadBalancingShortestPath(Graph<V, E> g, int k) {
		super(g, k);
		this.results = new LinkedList<NuptPath>();
		this.candidates = new LinkedList<NuptPath>();
	}

	@Override
	public Map<V, E> getIncomingEdgeMap(V source) {
		return null;
	}

	/**
	 * Returns a <code>List</code> of the {@link NuptPath} on the shortest path
	 * from <code>source</code> to <code>target</code>, in order of their
	 * occurrence on this path.
	 * 
	 * @param source
	 *            the vertex from which distances are to be measured
	 * @param target
	 *            the vertex to which distances are to be measured
	 * @return a <code>List</code> of {@link NuptPath} which indicates the K-Top
	 *         shortest paths from source{# source} to target{# target}
	 */
	public List<NuptPath> getPath(V source, V target) {
		this.source = source;
		this.target = target;
		prepare();
		int count = 1;
		while (hasNext() && count < this.kTop) {
			next();
			count++;
		}
		return this.results;
	}

	/**
	 * Calculates the first shortest path by Dijkstra's algorithm and then
	 * provides seed for the Yen's algorithm.
	 */
	private void prepare() {
		System.out.println("prepare:" + this.g);
		NuptPath firstPath = getShortestPathByDijkstra(source, target);
		System.out.println("firstPath:" + firstPath);
		this.results.add(firstPath);
	}

	/**
	 * Determines whether it is necessary to continue the iteration.
	 * 
	 * @return boolean type which indicates determination result
	 */
	private boolean hasNext() {
		return this.more;
	}

	/**
	 * Implements Yen's single-source K-shortest algorithm for an acyclic graph
	 * with non-negative edge cost.
	 * 
	 * The algorithm can be broken down into two parts, determining the first
	 * k-shortest path A[1], and then determining all other k-shortest paths. It
	 * is assumed that the container A will hold the k-shortest path, whereas
	 * the container B, will hold the potential k-shortest paths. To determine
	 * A[1], the shortest path from the source to the sink, any efficient
	 * shortest path algorithm can be used.
	 * 
	 * To find the A[k], where k ranges from 2 to K, the algorithm assumes that
	 * all paths from A[1] to A[k-1] have previously been found. The k iteration
	 * can be divided into two processes, finding all the deviations A[k][i] and
	 * choosing a minimum length path to become A[k]. Note that in this
	 * iteration,i ranges from 1 to the size of A[k] minus 1.
	 * 
	 * The first process can be further subdivided into three operations,
	 * choosing the R[k][i], finding S[k][i], and then adding A[k][i] to the
	 * container B. The root path, R[k][i], is chosen by finding the sub-path in
	 * A[k-1] that follows the first i nodes of A[j], where j ranges from 1 to
	 * k-1. Then, if a path is found, the cost of edge d[i][i+1] of A[j] is set
	 * to infinity. Next, the spur path, S[k][i], is found by computing the
	 * shortest path from the spur node, node i, to the sink. The removal of
	 * previous used edges from (i) to (i+1) ensures that the spur path is
	 * different.A[k][i]=R[k][i]+S[K][i], the addition of the root path and the
	 * spur path, is added to B. Next, the edges that were removed, i.e. had
	 * their cost set to infinity, are restored to their initial values.
	 * 
	 * The second process determines a suitable path for A[K] by finding the
	 * path in container B with the lowest cost. This path is removed from
	 * container B and inserted into container A and the algorithm continues to
	 * the next iteration. Note that if the amount of paths in container B equal
	 * or exceed the amount of k-shortest paths that still need to be found,
	 * then the necessary paths of container B are added to container A and the
	 * algorithm is finished.
	 * 
	 * space complexity: To store the edges of the graph, the shortest path list
	 * A, and the potential shortest path list B,N^2+KN memory addresses are
	 * required,where N is the amount of nodes in the graph.
	 * 
	 * time complexity: The time complexity of Yen's algorithm is dependent on
	 * the shortest path algorithm used in the computation of the spur paths, so
	 * the Dijkstra algorithm is assumed.At worse case,the time complexity
	 * becomes O(KN(M+NlogN)),where M is the amount of edges in the graph.
	 * 
	 * It is worth noting that even if the Dijkstra algorithm does not calculate
	 * the shortest path, the deleted nodes and edges should be restored to
	 * avoid affecting the next iteration.
	 */
	@SuppressWarnings("unchecked")
	private void next() {
		System.out.println("the graph before starting to calculate:" + this.g);
		NuptPath curPath = this.results.get(this.results.size() - 1);
		int size = curPath.nodeSize();
		Node spurVertex = null;
		NuptPath rootPath = null;
		List<V> removedNodes = new LinkedList<V>();
		Map<E, Pair<V>> removedMap = new HashMap<E, Pair<V>>();
		E remoEdge = null;
		V remoNode = null;
		for (int i = 0; i < size - 1; i++) {
			// System.out.println("before:"+g);
			removedNodes.clear();
			removedMap.clear();
			spurVertex = curPath.getNode(i);
			rootPath = curPath.getSubPathByNodeIndex(0, i);
			for (NuptPath path : this.results) {
				if (i <= path.size() && rootPath.equals(path.getSubPathByNodeIndex(0, i))) {
					// remove (i,i+1) from graph
					// System.out.println("edge coming
					// remove:"+this.g.getEdge(s,t));
					remoEdge = (E) path.getEdge(i);
					if (remoEdge != null) {
						System.out.println("from 1 section:" + ((SparseMultigraph<V, E>) g).getEndpoints(remoEdge));
						if (((SparseMultigraph<V, E>) g).getEndpoints(remoEdge) != null) {
							removedMap.put(remoEdge, ((SparseMultigraph<V, E>) g).getEndpoints(remoEdge));
						}
						// removedEdges.add(remoEdge);
						this.g.removeEdge(remoEdge);
					}
				}
			}
			Collection<E> incidentEdges = null;

			for (Node v : rootPath.getNodes()) {
				// remove v from graph
				if (v != spurVertex) {
					remoNode = (V) v;
					System.out.println("nodes removed:");
					incidentEdges = this.g.getIncidentEdges(remoNode);
					if (incidentEdges != null) {
						for (E e : incidentEdges) {
							System.out.println("from 2 section:" + ((SparseMultigraph<V, E>) g).getEndpoints(e));
							removedMap.put(e, ((SparseMultigraph<V, E>) g).getEndpoints(e));
						}
					}
					removedNodes.add(remoNode);
					this.g.removeVertex(remoNode);
				}
			}

			// calculate the shortest path between spur node and target node
			// based on Dijkstra
			System.out.println("the graph before getShortestPathByDijkstra:" + this.g);
			NuptPath subPath = getShortestPathByDijkstra((V) spurVertex, this.target);
			System.out.println("the graph after getShortestPathByDijkstra NuptPath:");
			System.out.println(subPath);
			if (subPath != null) {
				// merge path newPath=rootPath+subPath
				NuptPath newPath = merge(rootPath, subPath);
				if (!this.candidates.contains(newPath)) {
					this.candidates.add(newPath);
				}
			}

			System.out.println("the graph before restoring:" + this.g);
			// restore nodes in rootPath to graph;
			for (V v : removedNodes) {
				this.g.addVertex(v);
			}
			// restore edges to graph;
			for (Entry<E, Pair<V>> entry : removedMap.entrySet()) {
				System.out.println("from 3 section:" + entry.getValue());
				this.g.addEdge(entry.getKey(), entry.getValue(), EdgeType.DIRECTED);
			}

			System.out.println("the graph after restoring:" + this.g);
		}
		/**
		 * This judgment is necessary, because it determines whether it is
		 * necessary to iterate. If there is no splittable path in the candidate
		 * set, the iteration is terminated.
		 */
		if (this.candidates.isEmpty()) {
			this.more = false;
			return;
		}

		Integer min = Integer.MAX_VALUE;
		NuptPath newPath = null;
		int wh = 0;
		for (NuptPath p : this.candidates) {
			wh = weightSum(p);
			if (wh < min) {
				min = wh;
				newPath = p;
			}
		}
		// System.out.println("newPath:"+(newPath==null))

		this.results.add(newPath);
		this.candidates.remove(newPath);
	}

	/**
	 * Computes the weight of a <code>path</code>{@link NuptPath}.
	 * 
	 * @param path
	 *            a {@link NuptPath} instance that is prepared to be computed
	 * @return the weight of the given {@link NuptPath} <code>path</code>
	 */
	public int weightSum(NuptPath path) {
		int result = 0;
		for (Edge edge : path.getEdges()) {
			result += this.transformer.transform(edge).intValue();
		}
		return result;
	}

	/**
	 * Completes the merge of two paths {@link NuptPath}.Under the legal
	 * conditions of the two paths, <code>before</code>as the former of the new
	 * path, and <code>after</code> as the latter.
	 * 
	 * @param before
	 *            a former {@link NuptPath} instance to which is prepared to
	 *            merged
	 * @param after
	 *            a latter {@link NuptPath} instance to which is prepared to
	 *            merged
	 * @return a new {@link NuptPath} instance from which two paths are merged
	 */
	@SuppressWarnings("unchecked")
	public NuptPath merge(NuptPath before, NuptPath after) {
		if (before == null && after == null) {
			return null;
		}
		if (before != null && after == null) {
			return before;
		}
		if (before == null && after != null) {
			return after;
		}

		if (before.size() >= 0 && after.size() == 0) {
			return before;
		}

		if (before.size() == 0 && after.size() > 0) {
			return after;
		}

		Node tail = before.lastNode();
		Node head = after.firstNode();
		NuptPath newPath = null;
		try {
			newPath = new NuptPath(before);
		} catch (ConstructionException e) {
			e.printStackTrace();
		}

		if (tail == head) {
			for (Edge edge : after.getEdges()) {
				newPath.append(edge);
			}
		} else {
			Collection<V> col = this.g.getSuccessors((V) tail);
			Node temp = null;
			for (V v : col) {
				temp = (Node) v;
				// check the continuity between NuptPath before and NuptPath
				// after
				if (temp == head) {
					// get and add edge
					newPath.append((Edge) this.g.findEdge((V) tail, v));
					for (Edge edge : after.getEdges()) {
						newPath.append(edge);
					}
					break;
				}
			}

		}

		return newPath;

	}

	@Override
	protected LoadBalancingDistance<V, E>.SourceData getSourceData(V source) {
		SourceData sd = sourceMap.get(source);
		if (sd == null)
			sd = new SourcePathData(source);
		return sd;
	}

	/**
	 * @Project: loadbalancing
	 * @Description: For a given source vertex, holds the estimated and final
	 *               distances, tentative and final assignments of incoming
	 *               edges on the shortest path from the source vertex, and a
	 *               priority queue (ordered by estimaed distance) of the
	 *               vertices for which distances are unknown.
	 * @see {@link edu.uci.ics.jung.algorithms.shortestpath.DijkstraShortestPath.SourcePathData<V,
	 *      E>}
	 * @Author: Yanjun Wang
	 * @Date: 2017年3月8日
	 */
	protected class SourcePathData extends SourceData {
		protected Map<V, E> tentativeIncomingEdges;
		protected LinkedHashMap<V, E> incomingEdges;

		protected SourcePathData(V source) {
			super(source);
			incomingEdges = new LinkedHashMap<V, E>();
			tentativeIncomingEdges = new HashMap<V, E>();
		}

		@Override
		public void update(V dest, E tentative_edge, double new_dist) {
			super.update(dest, tentative_edge, new_dist);
			tentativeIncomingEdges.put(dest, tentative_edge);
		}
		

		@Override
		public Map.Entry<V, Number> getNextVertex() {
			Map.Entry<V, Number> p = super.getNextVertex();
			V v = p.getKey();
			System.out.println("next node:" + v);
			System.out.println("weight of n->s:" + p.getValue());
			System.out.println("contains v:" + tentativeIncomingEdges.containsKey(v));
			System.out.println("tentativeIncomingEdges:" + tentativeIncomingEdges);
			E incoming = tentativeIncomingEdges.remove(v);
			incomingEdges.put(v, incoming);
			System.out.println("V:" + v);
			System.out.println("incoming:" + incoming);
			return p;
		}

		@Override
		public void restoreVertex(V v, double dist) {
			super.restoreVertex(v, dist);
			E incoming = incomingEdges.get(v);
			tentativeIncomingEdges.put(v, incoming);
		}

		@Override
		public void createRecord(V w, E e, double new_dist) {
			super.createRecord(w, e, new_dist);
			tentativeIncomingEdges.put(w, e);
		}

	}

}
