/*
 * @Copyright (c) 2017 Nanjing University Of Posts And Telecommunications (NUPT).  All rights reserved.
 */
package edu.nupt.loadbalancing.beans;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.Transformer;
import org.apache.commons.collections15.functors.ConstantTransformer;
import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;

import edu.nupt.loadbalancing.beans.LoadBalancingShortestPath.SourcePathData;
import edu.uci.ics.jung.algorithms.shortestpath.Distance;
import edu.uci.ics.jung.algorithms.util.BasicMapEntry;
import edu.uci.ics.jung.algorithms.util.MapBinaryHeap;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.Hypergraph;
import edu.uci.ics.jung.graph.SparseMultigraph;

/**
 * @Project: loadbalancing
 * @Description: The class is the parent class of the shortest path algorithm
 *               {@link edu.nupt.loadbalancing.beans.LoadBalancingShortestPath<V,
 *               E>} based on the load balancing strategy, and also implements
 *               interface
 *               {@link edu.uci.ics.jung.algorithms.shortestpath.Distance<V>} to
 *               facilitate the invocation of other modules in OpenDayLight
 *               {@link https://www.opendaylight.org/}.
 * @Author: Yanjun Wang
 * @Date: 2017年3月17日
 */
public class LoadBalancingDistance<V, E> implements Distance<V> {
	protected Hypergraph<V, E> g;
	protected Map<V, SourceData> sourceMap;
	protected Transformer<Edge, ? extends Number> transformer;
	protected V source;
	protected V target;
	protected int kTop = 0;
	protected boolean cached;
	protected double max_distance;
	protected int max_targets;

	/**
	 * Creates a LoadBalancingDistance instance based on a given global network
	 * graph {# g:{@link edu.uci.ics.jung.graph.Graph<V, E>}} and the parameter
	 * k {# k:the order of K-Top shortest path algorithm}.
	 * 
	 * @param g
	 *            a graph of global network
	 * @param k
	 *            the order of K-Top shortest algorithm
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public LoadBalancingDistance(Graph<V, E> g, int k) {
		super();
		this.g = g;
		this.kTop = k;
		this.sourceMap = new HashMap<V, SourceData>();
		this.transformer = new ConstantTransformer(1);
		this.max_distance = Double.POSITIVE_INFINITY;
		this.max_targets = Integer.MAX_VALUE;
	}

	public Number getDistance(Object source, Object target) {
		// TODO Auto-generated method stub
		return null;
	}

	public Map<V, Number> getDistanceMap(Object source) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Returns a <code>NuptPath instance</code> contains the edges on the
	 * shortest path from source{# source} to target{# target}, in order of
	 * their occurrence on this path. If either vertex is not in the graph for
	 * which this instance was created, throws <code>
	 * IllegalArgumentException</code>.
	 *
	 * @param source
	 *            the vertex from which distances are to be measured
	 * @param target
	 *            the vertex to which distances are to be measured
	 * @return a NuptPath instance which indicates a shortest path from source{#
	 *         source} to target{# target}
	 */
	public NuptPath getShortestPathByDijkstra(V source, V target) {
		// System.out.println("vertexs before:"+this.g.getVertices());
		if (!g.containsVertex(source))
			throw new IllegalArgumentException("Specified source vertex " + source + " is not part of graph " + g);

		if (!g.containsVertex(target))
			throw new IllegalArgumentException("Specified target vertex " + target + " is not part of graph " + g);

		LinkedList<Edge> path = new LinkedList<Edge>();
		NuptPath newPath = null;
		// collect path data; must use internal method rather than
		// calling getIncomingEdge() because getIncomingEdge() may
		// wipe out results if results are not cached
		Set<V> targets = new HashSet<V>();
		targets.add(target);
		singleSourceShortestPath(source, targets, g.getVertexCount(), true);
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Map<V, E> incomingEdges = ((SourcePathData) sourceMap.get(source)).incomingEdges;
		System.out.println("incomingEdges:" + incomingEdges);
		if (incomingEdges.isEmpty() || incomingEdges.get(target) == null)
			return newPath;

		V current = target;
		while (!current.equals(source)) {
			E incoming = incomingEdges.get(current);
			path.addFirst((Edge) incoming);
			// System.out.println("current:"+current);
			// System.out.println("incoming:"+incoming);
			current = ((SparseMultigraph<V, E>) g).getOpposite(current, incoming);
			// current = getOpposite(current, incoming);
		}
		try {
			newPath = new NuptPath(path);
		} catch (ConstructionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return newPath;
	}

	/**
	 * Gets a vertex from the given edge{# e},and the vertex shares the edge
	 * <code>e {# e}</code> with the given vertex {# v}
	 * 
	 * @param v
	 *            a vertex that is opposite to the return value
	 * @param e
	 *            an edge which connects two vertexes
	 * @return the vertex that is opposite to {# v} in the edge {# e}
	 */
	@SuppressWarnings("unused")
	public V getOpposite(V v, E e) {
		Edge edge = (Edge) e;
		Node n = (Node) v;
		if (edge.getTailNodeConnector().getNode() == n) {
			return (V) edge.getHeadNodeConnector().getNode();
		} else if (edge.getHeadNodeConnector().getNode() == n) {
			return (V) edge.getTailNodeConnector().getNode();
		} else {
			return null;
		}

	}

	/**
	 * Implements Dijkstra's single-source shortest-path algorithm for weighted
	 * graphs. Uses a <code>MapBinaryHeap</code> as the priority queue, which
	 * gives this algorithm a time complexity of O(m lg n) (m = # of edges, n =
	 * # of vertices). This algorithm will terminate when any of the following
	 * have occurred (in order of priority):
	 * <ul>
	 * <li>the distance to the specified target (if any) has been found
	 * <li>no more vertices are reachable
	 * <li>the specified # of distances have been found, or the maximum distance
	 * desired has been exceeded
	 * <li>all distances have been found
	 * </ul>
	 * 
	 * @param source
	 *            the vertex from which distances are to be measured
	 * @param numDests
	 *            the number of distances to measure
	 * @param targets
	 *            the set of vertices to which distances are to be measured
	 */
	public LinkedHashMap<V, Number> singleSourceShortestPath(V source, Collection<V> targets, int numDests,
			boolean regular) {
		reset(source);
		SourceData sd = getSourceData(source);

		Set<V> to_get = new HashSet<V>();
		if (targets != null) {
			to_get.addAll(targets);
			Set<V> existing_dists = sd.distances.keySet();
			for (V o : targets) {
				if (existing_dists.contains(o))
					to_get.remove(o);
			}
		}

		// if we've exceeded the max distance or max # of distances we're
		// willing to calculate, or
		// if we already have all the distances we need,
		// terminate
		if (sd.reached_max || (targets != null && to_get.isEmpty()) || (sd.distances.size() >= numDests)) {
			return sd.distances;
		}

		while (!sd.unknownVertices.isEmpty() && (sd.distances.size() < numDests || !to_get.isEmpty())) {
			System.out.println("current node:" + source);
			Map.Entry<V, Number> p = sd.getNextVertex();
			V v = p.getKey();
			double v_dist = p.getValue().doubleValue();
			to_get.remove(v);

			if (v_dist > this.max_distance) {
				// we're done; put this vertex back in so that we're not
				// including
				// a distance beyond what we specified
				sd.restoreVertex(v, v_dist);
				sd.reached_max = true;
				break;
			}
			sd.dist_reached = v_dist;
			if (sd.distances.size() >= this.max_targets) {
				sd.reached_max = true;
				break;
			}

			for (E e : getEdgesToCheck(v)) {
				for (V w : g.getIncidentVertices(e)) {
					if (!sd.distances.containsKey(w)) {
						double edge_weight = this.transformer.transform((Edge) e).doubleValue();
						if (edge_weight < 0)
							throw new IllegalArgumentException("Edges weights must be non-negative");
						double new_dist;
						if (regular == true) {
							new_dist = v_dist + edge_weight;
						} else {
							if (v_dist <= edge_weight) {
								new_dist = edge_weight;
							} else {
								new_dist = v_dist;
							}
						}
						if (!sd.estimatedDistances.containsKey(w)) {
							sd.createRecord(w, e, new_dist);
						} else {
							double w_dist = ((Double) sd.estimatedDistances.get(w)).doubleValue();
							if (new_dist < w_dist) // update tentative distance
													// & path for w
								sd.update(w, e, new_dist);
						}
					}
				}
			}
		}
		return sd.distances;
	}

	/**
	 * Clears all stored distances for the specified source vertex
	 * <code>source{# source}</code>. Should be called whenever the stored
	 * distances from this vertex are invalidated by changes to the graph.
	 * 
	 * @param source
	 *            the vertex that is prepared to be cleared all stored distances
	 */
	public void reset(V source) {
		sourceMap.put(source, null);
	}

	/**
	 * Clears all stored distances for this instance. Should be called whenever
	 * the graph is modified (edge weights changed or edges added/removed). If
	 * the user knows that some currently calculated distances are unaffected by
	 * a change, <code>reset(V)</code> may be appropriate instead.
	 */
	public void reset() {
		sourceMap = new HashMap<V, SourceData>();
	}

	/**
	 * Gets a {@link SourceData } instance for the given
	 * <code>source {# source}</code>. If the SourceData instance corresponding
	 * to <code>source {# source}</code> is not recorded in @see
	 * {@link #sourceMap}, a new SourceData instance is created and 'key-value'
	 * is stored in @see {@link #sourceMap}.
	 * 
	 * @param source
	 *            the vertex to which is prepared to measured
	 * @return a {@link SourceData} instance corresponding to
	 *         <code>source {# source}</code>
	 */
	protected SourceData getSourceData(V source) {
		SourceData sd = sourceMap.get(source);
		if (sd == null)
			sd = new SourceData(source);
		return sd;
	}

	/**
	 * Returns the set of edges incident to <code>v</code> that should be
	 * tested. By default, this is the set of outgoing edges for instances of
	 * <code>Graph</code>, the set of incident edges for instances of
	 * <code>Hypergraph</code>, and is otherwise undefined.
	 * 
	 * @param v
	 *          the vertex to which is prepared to measured
	 * @return 
	 * 			a set of edges incident to <code>v{# v}</code>
	 */
	protected Collection<E> getEdgesToCheck(V v) {
		/*
		 * the vertices member of SparseMultigraph, the key is vertex, value is
		 * Pair which's first member is set about incomingEdges and second
		 * member is set about outingEdges.
		 * 
		 */
		return g instanceof Graph ? ((Graph<V, E>) g).getOutEdges(v) : g.getIncidentEdges(v);
	}

	/**
	 * @Project: loadbalancing
	 * @Description: Compares according to distances, so that the BinaryHeap
	 *               knows how to order the tree.
	 * @see {@link edu.uci.ics.jung.algorithms.shortestpath.DijkstraDistance.VertexComparator<V,
	 *      E>}
	 * @Author: Yanjun Wang
	 * @Date: 2017年3月17日
	 */
	protected static class VertexComparator<V> implements Comparator<V> {
		private Map<V, Number> distances;

		protected VertexComparator(Map<V, Number> distances) {
			this.distances = distances;
		}

		public int compare(V o1, V o2) {
			return ((Double) distances.get(o1)).compareTo((Double) distances.get(o2));
		}
	}

	/**
	 * @Project: loadbalancing
	 * @Description: For a given source vertex, holds the estimated and final
	 *               distances, tentative and final assignments of incoming
	 *               edges on the shortest path from the source vertex, and a
	 *               priority queue (ordered by estimated distance) of the
	 *               vertices for which distances are unknown.
	 * @see {@link edu.uci.ics.jung.algorithms.shortestpath.DijkstraDistance.SourceData<V,
	 *      E>}
	 * @Author: Yanjun Wang
	 * @Date: 2017年3月17日
	 */
	protected class SourceData {
		protected LinkedHashMap<V, Number> distances;
		protected Map<V, Number> estimatedDistances;
		protected MapBinaryHeap<V> unknownVertices;
		protected boolean reached_max = false;
		protected double dist_reached = 0;

		protected SourceData(V source) {
			distances = new LinkedHashMap<V, Number>();
			estimatedDistances = new HashMap<V, Number>();
			unknownVertices = new MapBinaryHeap<V>(new VertexComparator<V>(estimatedDistances));

			sourceMap.put(source, this);

			// initialize priority queue
			estimatedDistances.put(source, new Double(0)); // distance from
															// source to itself
															// is 0
			unknownVertices.add(source);
			reached_max = false;
			dist_reached = 0;
		}

		protected Map.Entry<V, Number> getNextVertex() {
			V v = unknownVertices.remove();
			Double dist = (Double) estimatedDistances.remove(v);
			distances.put(v, dist);
			return new BasicMapEntry<V, Number>(v, dist);
		}

		protected void update(V dest, E tentative_edge, double new_dist) {
			estimatedDistances.put(dest, new_dist);
			unknownVertices.update(dest);
		}

		protected void createRecord(V w, E e, double new_dist) {
			estimatedDistances.put(w, new_dist);
			unknownVertices.add(w);
		}

		protected void restoreVertex(V v, double dist) {
			unknownVertices.add(v);
			estimatedDistances.put(v, dist);
			distances.remove(v);
		}
	}

}
