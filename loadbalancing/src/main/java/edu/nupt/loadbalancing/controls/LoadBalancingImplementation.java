/*
 * @Copyright (c) 2017 Nanjing University Of Posts And Telecommunications (NUPT).  All rights reserved.
 */
package edu.nupt.loadbalancing.controls;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.opendaylight.controller.clustering.services.IClusterContainerServices;
import org.opendaylight.controller.sal.core.Bandwidth;
import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.controller.sal.core.Property;
import org.opendaylight.controller.sal.core.UpdateType;
import org.opendaylight.controller.sal.reader.NodeConnectorStatistics;
import org.opendaylight.controller.sal.routing.IListenRoutingUpdates;
import org.opendaylight.controller.sal.routing.IRouting;
import org.opendaylight.controller.sal.topology.TopoEdgeUpdate;
import org.opendaylight.controller.statisticsmanager.IStatisticsManager;
import org.opendaylight.controller.switchmanager.ISwitchManager;
import org.opendaylight.controller.topologymanager.ITopologyManager;
import org.opendaylight.controller.topologymanager.ITopologyManagerClusterWideAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nupt.loadbalancing.beans.LoadBalancingShortestPath;
import edu.nupt.loadbalancing.beans.NuptPath;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.SparseMultigraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import edu.uci.ics.jung.graph.util.Pair;

/**
 * 
 * @Project: loadbalancing
 * @Description: Implement of shortest path based on load balancing strategy.
 * @Author: Yanjun Wang
 * @Date: 2017年3月17日
 */
public class LoadBalancingImplementation implements IRouting, ITopologyManagerClusterWideAware {
	private static Logger log = LoggerFactory.getLogger(LoadBalancingImplementation.class);
	private ConcurrentMap<Short, Graph<Node, Edge>> topologyBWAware;
	private ConcurrentMap<Short, LoadBalancingShortestPath<Node, Edge>> sptBWAware;
	private Set<IListenRoutingUpdates> routingAware;
	private ISwitchManager switchManager;
	private ITopologyManager topologyManager;
	private IStatisticsManager statisticsManager;
	private IClusterContainerServices clusterContainerService;
	private DataStatisticsExecutor dataStatisticsExecutor;
	private static final long DEFAULT_LINK_SPEED = Bandwidth.BW100Mbps;
	private static final int DEFAULT_KTOP = 5;

	/*
	 * Configuration methods
	 * 
	 */
	public void setListenRoutingUpdates(final IListenRoutingUpdates i) {
		if (this.routingAware == null) {
			this.routingAware = new HashSet<IListenRoutingUpdates>();
		}
		if (this.routingAware != null) {
			log.debug("Adding routingAware listener: {}", i);
			this.routingAware.add(i);
		}
	}

	public void unsetListenRoutingUpdates(final IListenRoutingUpdates i) {
		if (this.routingAware == null) {
			return;
		}
		log.debug("Removing routingAware listener");
		this.routingAware.remove(i);
		if (this.routingAware.isEmpty()) {
			this.routingAware = null;
		}
	}

	public void setSwitchManager(ISwitchManager switchManager) {
		this.switchManager = switchManager;
	}

	public void unsetSwitchManager(ISwitchManager switchManager) {
		if (this.switchManager == switchManager) {
			this.switchManager = null;
		}
	}

	public void setTopologyManager(ITopologyManager tm) {
		this.topologyManager = tm;
	}

	public void unsetTopologyManager(ITopologyManager tm) {
		if (this.topologyManager == tm) {
			this.topologyManager = null;
		}
	}

	public void setStatisticsManager(IStatisticsManager sm) {
		this.statisticsManager = sm;
	}

	public void unsetStatisticsManager(IStatisticsManager sm) {
		if (this.statisticsManager == sm) {
			this.statisticsManager = null;
		}
	}

	void setClusterContainerService(IClusterContainerServices s) {
		log.debug("Cluster Service set");
		this.clusterContainerService = s;
	}

	void unsetClusterContainerService(IClusterContainerServices s) {
		if (this.clusterContainerService == s) {
			log.debug("Cluster Service removed!");
			this.clusterContainerService = null;
		}
	}

	/*
	 * Function mathods
	 * 
	 */
	/**
	 * Updates the total network.
	 * 
	 */
	@SuppressWarnings({ "unchecked" })
	private synchronized boolean updateTopo(Edge edge, Short bw, UpdateType type) {
		Graph<Node, Edge> topo = this.topologyBWAware.get(bw);
		LoadBalancingShortestPath<Node, Edge> spt = this.sptBWAware.get(bw);
		boolean edgePresentInGraph = false;
		Short baseBW = Short.valueOf((short) 0);

		if (topo == null) {
			// Create topology for this BW
			Graph<Node, Edge> g = new SparseMultigraph<Node, Edge>();
			this.topologyBWAware.put(bw, g);
			topo = this.topologyBWAware.get(bw);
			this.sptBWAware.put(bw, new LoadBalancingShortestPath<Node, Edge>(g, DEFAULT_KTOP));
			spt = this.sptBWAware.get(bw);
		}

		if (topo != null) {
			NodeConnector src = edge.getTailNodeConnector();
			NodeConnector dst = edge.getHeadNodeConnector();
			if (spt == null) {
				spt = new LoadBalancingShortestPath<Node, Edge>(topo, DEFAULT_KTOP);
				this.sptBWAware.put(bw, spt);
			}

			switch (type) {
			case ADDED:
				// Make sure the vertex are there before adding the edge
				topo.addVertex(src.getNode());
				topo.addVertex(dst.getNode());
				// Add the link between
				edgePresentInGraph = topo.containsEdge(edge);
				if (edgePresentInGraph == false) {
					try {
						topo.addEdge(new Edge(src, dst), src.getNode(), dst.getNode(), EdgeType.DIRECTED);
					} catch (final ConstructionException e) {
						log.error("", e);
						return edgePresentInGraph;
					}
				}
			case CHANGED:
				// Mainly raised only on properties update, so not really useful
				// in this case
				break;
			case REMOVED:
				// Remove the edge
				try {
					topo.removeEdge(new Edge(src, dst));
				} catch (final ConstructionException e) {
					log.error("", e);
					return edgePresentInGraph;
				}

				// If the src and dst vertex don't have incoming or
				// outgoing links we can get ride of them
				if (topo.containsVertex(src.getNode()) && (topo.inDegree(src.getNode()) == 0)
						&& (topo.outDegree(src.getNode()) == 0)) {
					log.debug("Removing vertex {}", src);
					topo.removeVertex(src.getNode());
				}

				if (topo.containsVertex(dst.getNode()) && (topo.inDegree(dst.getNode()) == 0)
						&& (topo.outDegree(dst.getNode()) == 0)) {
					log.debug("Removing vertex {}", dst);
					topo.removeVertex(dst.getNode());
				}
				break;
			}
			spt.reset();
			if (bw.equals(baseBW)) {
				clearMaxThroughput();
			}
		} else {
			log.error("Cannot find topology for BW {} this is unexpected!", bw);
		}
		return edgePresentInGraph;
	}

	/*
	 * 
	 * some test methods
	 */
	public void removeNode(Node node) {
		Graph<Node, Edge> g = this.topologyBWAware.get(Short.valueOf((short) 0));
		g.removeVertex(node);
	}

	public void removeEdge(Edge edge) {
		Graph<Node, Edge> g = this.topologyBWAware.get(Short.valueOf((short) 0));
		g.removeEdge(edge);
	}

	public void addNode(Node node) {
		Graph<Node, Edge> g = this.topologyBWAware.get(Short.valueOf((short) 0));
		g.addVertex(node);
	}

	public void addEdge(Edge edge) {
		Graph<Node, Edge> g = this.topologyBWAware.get(Short.valueOf((short) 0));
		g.addEdge(edge, new Pair<Node>(edge.getTailNodeConnector().getNode(), edge.getHeadNodeConnector().getNode()),
				EdgeType.DIRECTED);
	}

	public void printGraph() {
		System.out.println(this.topologyBWAware.get(Short.valueOf((short) 0)));
	}

	/**
	 * 
	 * Updates the given edge.
	 */
	private boolean edgeUpdate(Edge e, UpdateType type, Set<Property> props, boolean local) {
		String srcType = null;
		String dstType = null;

		log.trace("Got an edgeUpdate: {} props: {} update type: {} local: {}", new Object[] { e, props, type, local });

		if ((e == null) || (type == null)) {
			log.error("Edge or Update type are null!");
			return false;
		} else {
			srcType = e.getTailNodeConnector().getType();
			dstType = e.getHeadNodeConnector().getType();

			if (srcType.equals(NodeConnector.NodeConnectorIDType.PRODUCTION)) {
				log.debug("Skip updates for {}", e);
				return false;
			}

			if (dstType.equals(NodeConnector.NodeConnectorIDType.PRODUCTION)) {
				log.debug("Skip updates for {}", e);
				return false;
			}
		}

		Bandwidth bw = new Bandwidth(0);
		boolean newEdge = false;
		if (props != null) {
			props.remove(bw);
		}

		Short baseBW = Short.valueOf((short) 0);
		// Update base topo
		newEdge = !updateTopo(e, baseBW, type);
		if (newEdge == true) {
			if (bw.getValue() != baseBW) {
				// Update BW topo
				updateTopo(e, (short) bw.getValue(), type);
			}
		}
		return newEdge;
	}

	/**
	 * Updates all edges according to the given UPDATE set.
	 * 
	 */
	public void edgeUpdate(List<TopoEdgeUpdate> topoedgeupdateList) {
		log.trace("Start of a Bulk EdgeUpdate with " + topoedgeupdateList.size() + " elements");
		boolean callListeners = false;
		for (int i = 0; i < topoedgeupdateList.size(); i++) {
			Edge e = topoedgeupdateList.get(i).getEdge();
			Set<Property> p = topoedgeupdateList.get(i).getProperty();
			UpdateType type = topoedgeupdateList.get(i).getUpdateType();
			boolean isLocal = topoedgeupdateList.get(i).isLocal();
			if ((edgeUpdate(e, type, p, isLocal)) && (!callListeners)) {
				callListeners = true;
			}
		}

		// The routing listeners should only be called on the coordinator, to
		// avoid multiple controller cluster nodes to actually do the
		// recalculation when only one need to react
		boolean amICoordinator = true;
		if (this.clusterContainerService != null) {
			amICoordinator = this.clusterContainerService.amICoordinator();
		}
		if ((callListeners) && (this.routingAware != null) && amICoordinator) {
			log.trace("Calling the routing listeners");
			for (IListenRoutingUpdates ra : this.routingAware) {
				try {
					ra.recalculateDone();
				} catch (Exception ex) {
					log.error("Exception on routingAware listener call", ex);
				}
			}
		}
		log.trace("End of a Bulk EdgeUpdate");
		System.out.println("graph:" + this.topologyBWAware.get(Short.valueOf((short) 0)));
	}

	/**
	 * Entry called by external service.
	 */
	public Path getRoute(Node src, Node dst) {
		if ((src == null) || (dst == null)) {
			return null;
		}
		return getRoute(src, dst, (short) 0);
	}

	/**
	 * 
	 */
	public Path getMaxThroughputRoute(Node src, Node dst) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 
	 */
	public Path getRoute(Node src, Node dst, Short Bw) {
		LoadBalancingShortestPath<Node, Edge> spt = this.sptBWAware.get(Bw);
		if (spt == null) {
			log.info("Algorithm about shortest path is not found.");
			return null;
		}
		List<Edge> path = null;
		List<NuptPath> paths = spt.getPath(src, dst);
		path = this.dataStatisticsExecutor.getOptimalPath(paths).getEdges();

		int i = 1;
		for (NuptPath np : paths) {
			System.out.println("index of path [" + (i++) + "]:" + np);
		}
		// System.out.println("**************************"+paths.get(0).equals(paths.get(1)));
		Path res;
		try {
			res = new Path(path);
		} catch (ConstructionException e) {
			log.debug("Caught an exception when construct a Path instance.");
			return null;
		}
		return res;
	}

	/**
	 * Clears all net graph
	 */
	public void clear() {
		LoadBalancingShortestPath<Node, Edge> spt;
		for (Short bw : this.sptBWAware.keySet()) {
			spt = this.sptBWAware.get(bw);
			if (spt != null) {
				spt.reset();
			}
		}

	}

	/**
	 * 
	 */
	public void clearMaxThroughput() {
		// TODO Auto-generated method stub

	}

	/**
	 * 
	 */
	public void initMaxThroughput(Map<Edge, Number> EdgeWeightMap) {
		// TODO Auto-generated method stub

	}

	/**
	 * 
	 */
	public void edgeOverUtilized(Edge edge) {

	}

	/**
	 * 
	 */
	public void edgeUtilBackToNormal(Edge edge) {

	}
	/*
	 * Life cycle methods
	 * 
	 */

	/**
	 * Function called by the dependency manager when all the required
	 * dependencies are satisfied
	 *
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void init() {
		log.debug("Routing init() is called");
		this.topologyBWAware = new ConcurrentHashMap<Short, Graph<Node, Edge>>();
		this.sptBWAware = new ConcurrentHashMap<Short, LoadBalancingShortestPath<Node, Edge>>();
		this.dataStatisticsExecutor = new DataStatisticsExecutor();
		// Now create the default topology, which doesn't consider the
		// BW, also create the corresponding Dijkstra calculation
		Graph<Node, Edge> g = new SparseMultigraph();
		Short sZero = Short.valueOf((short) 0);
		this.topologyBWAware.put(sZero, g);
		this.sptBWAware.put(sZero, new LoadBalancingShortestPath(g, DEFAULT_KTOP));
		// Topologies for other BW will be added on a needed base
	}

	/**
	 * Function called by the dependency manager when at least one dependency
	 * become unsatisfied or when the component is shutting down because for
	 * example bundle is being stopped.
	 *
	 */
	void destroy() {
		log.debug("Routing destroy() is called");
	}

	/**
	 * Function called by dependency manager after "init ()" is called and after
	 * the services provided by the class are registered in the service registry
	 *
	 */
	void start() {
		log.debug("Routing start() is called");
		// build the routing database from the topology if it exists.
		Map<Edge, Set<Property>> edges = topologyManager.getEdges();
		if (edges.isEmpty()) {
			return;
		}
		List<TopoEdgeUpdate> topoedgeupdateList = new ArrayList<TopoEdgeUpdate>();
		log.debug("Creating routing database from the topology");
		Iterator<Map.Entry<Edge, Set<Property>>> it = edges.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Edge, Set<Property>> entry = it.next();
			Edge e = entry.getKey();
			Set<Property> props = entry.getValue();
			TopoEdgeUpdate topoedgeupdate = new TopoEdgeUpdate(e, props, UpdateType.ADDED);
			topoedgeupdateList.add(topoedgeupdate);
		}
		edgeUpdate(topoedgeupdateList);
		this.dataStatisticsExecutor.start();
	}

	/**
	 * Function called by the dependency manager before the services exported by
	 * the component are unregistered, this will be followed by a "destroy ()"
	 * calls
	 *
	 */
	public void stop() {
		log.debug("Routing stop() is called");
	}

	/**
	 * @Project: loadbalancing
	 * @Description: The class defines the available methods for sampling the
	 *               network nodes statistics,so that the shortest path
	 *               algorithm is done with some sampling data. At the same
	 *               time, thread pool <code>
	 * ScheduledExecutorService</code> is used in the class instead of
	 *               <code>TimeTask</code> due to the later's disadvantages.
	 * @Author: Yanjun Wang
	 * @Date: 2017年3月10日
	 */
	class DataStatisticsExecutor {
		private ScheduledExecutorService scheduledThreadPool;
		/**
		 * The value of {@link #statisticsMap} is an array which's length is
		 * {{@link #TIME_SAMPLING_CYCLE}/{@link #TIME_SAMPLING_INTERVAL}+1}. The
		 * first element[0] represents the last data of corresponding
		 * {@link NodeConnector} in the last sampling cycle,and the remaining
		 * space of this array is used to store statistics for a
		 * {@link NodeConnector} at each sampling time in the current sampling
		 * cycle.Through this way,it is convenient that to calculate the
		 * difference between the first sampling value of the current sampling
		 * cycle and the last sampling value of the last sampling cycle to
		 * obtain the data amount of the corresponding {@link NodeConnector} at
		 * the first sampling time in the current sampling cycle.
		 */
		private ConcurrentHashMap<NodeConnector, long[]> statisticsMap;
		private static final long TIME_SAMPLING_INTERVAL = 10;
		private static final long TIME_SAMPLING_CYCLE = 100;
		private static final long TIME_SAMPLING_DELAY = 20;

		private DataStatisticsExecutor() {
			this.scheduledThreadPool = Executors.newScheduledThreadPool(1);
			this.statisticsMap = new ConcurrentHashMap<NodeConnector, long[]>();
		}

		private void start() {
			/**
			 * <code>DataStatisticsTimerTask</code> will be executed after
			 * {@link #TIME_SAMPLING_DELAY} seconds,and done with the
			 * {@link #TIME_SAMPLING_INTERVAL} seconds for the cycle.
			 * 
			 */
			this.scheduledThreadPool.scheduleWithFixedDelay(
					new StatisticsSamplingTimerTask(this.statisticsMap,
							(int) (TIME_SAMPLING_CYCLE / TIME_SAMPLING_INTERVAL)+1),
					TIME_SAMPLING_DELAY, TIME_SAMPLING_INTERVAL, TimeUnit.SECONDS);
		}

		/**
		 * Filters the shortest path with the lowest load rate from the given
		 * <code>List</code>. The load rate of the path depends on the highest
		 * load rate in all segments.
		 * 
		 * @param paths
		 *            a candidate list of paths
		 * @return a shortest path with the lowest load rate
		 */
		private NuptPath getOptimalPath(List<NuptPath> paths) {
			List<Edge> edges = null;
			NodeConnector tailConnector = null;
			NodeConnector headConnector = null;
			NuptPath result = null;
			double pathLoadRate = Integer.MAX_VALUE;
			for (NuptPath path : paths) {
				edges = path.getEdges();
				double edgeLoadRate = 0D;
				double temp = 0D;
				for (Edge edge : edges) {
					tailConnector = edge.getTailNodeConnector();
					headConnector = edge.getHeadNodeConnector();
					try {
						temp = calculateBWUtilizationRate(tailConnector, headConnector);
					} catch (Exception e) {
						log.error(e.getMessage());
					}
					if (temp > edgeLoadRate) {
						edgeLoadRate = temp;
					}
				}

				if (edgeLoadRate < pathLoadRate) {
					pathLoadRate = edgeLoadRate;
					result = path;
				}
			}

			return result;
		}

		/**
		 * Calculates the bandwidth usage of an edge which's ends are connected
		 * at node connector {# tail} and {# head} respectively. The bandwidth
		 * usage of an edge is roughly equal to:
		 * (Forwarding_Rate(nodeConnectorTail)+Forwarding_Rate(nodeConnectorHead))/Max_Bandwidth_of_Edge
		 * 
		 * @param tail
		 *            original nodeConnector
		 * @param head
		 *            terminal nodeConnector
		 * @return the bandwidth usage of an edge
		 * @throws Exception
		 *             throws an exception when tail or head is null
		 */
		private double calculateBWUtilizationRate(NodeConnector tail, NodeConnector head) throws Exception {
			if (tail == null || head == null) {
				// StringExpression message=StringFormatter.format("Fail to
				// calculate utilization rate of bandwidth about the edge
				// between tail { } and head { } due to tail or head
				// NodeConnector is null.", tail,head);
				throw new Exception(
						"Fail to calculate utilization rate of bandwidth about the edge between "
						+ "tail and head due to tail or head NodeConnector is null.");
			}
			long[] tailDatas = this.statisticsMap.get(tail);
			long[] headDatas = this.statisticsMap.get(head);
			double result = (calculateNCForwardingRate(tailDatas) + calculateNCForwardingRate(headDatas))
					/ DEFAULT_LINK_SPEED;
			return result;
		}

		/**
		 * Calculates the data forwarding rate for a port({@link NodeConnector}).
		 * The data forwarding rate of a port is roughly equal to:
		 * (Total_Amount_of_Data*Sampling_Interval)/Sampling_Cycle
		 * 
		 * @param datas
		 *            an array that storages statistics
		 * @return the data forwarding rate for a {@link NodeConnector}
		 */
		private double calculateNCForwardingRate(long[] datas) {
			long bytesSum = 0;
			for(int i=datas.length-1;i>0;i--){
				bytesSum+=datas[i]-datas[i-1];
			}
			double rate = (bytesSum * TIME_SAMPLING_INTERVAL) * 1.0 / TIME_SAMPLING_CYCLE;
			return rate;
		}

	}

	/**
	 * @Project: loadbalancing
	 * @Description: The class takes the responsibility for keeping the
	 *               NodeConnector statistics current and accurate, and takes
	 *               TIME_SAMPLING_INTERVAL as sampling interval.
	 * @Author: Yanjun Wang
	 * @Date: 2017年3月10日
	 */
	class StatisticsSamplingTimerTask implements Runnable {
		private ConcurrentHashMap<NodeConnector, long[]> statisticsMap;
		private int dataLength = 0;

		public StatisticsSamplingTimerTask(ConcurrentHashMap<NodeConnector, long[]> statisticsMap, int size) {
			super();
			this.statisticsMap = statisticsMap;
			this.dataLength = size;
		}

		public void run() {
			Graph<Node, Edge> g = topologyBWAware.get(0);
			Collection<Node> nodes = g.getVertices();
			List<NodeConnectorStatistics> ncsList = null;
			NodeConnector curNodeConnector = null;
			long[] datas = null;
			Long data =Long.valueOf(0);
			for (Node node : nodes) {
				ncsList = statisticsManager.getNodeConnectorStatistics(node);
				for (NodeConnectorStatistics ncs : ncsList) {
					curNodeConnector = ncs.getNodeConnector();
					data = Long.valueOf(ncs.getReceiveByteCount());
					datas = this.statisticsMap.get(curNodeConnector);
					if (datas == null) {
						datas = new long[dataLength];
						this.statisticsMap.put(curNodeConnector, datas);
					}
					updateData(datas, data);
				}
			}
		}

		/**
		 * Updates the historical data with new data. The real-time nature of
		 * the data improves with the increase of index.
		 * 
		 * @param datas
		 *            an array that storages historical statistics
		 * @param newData
		 *            a latest sampling data
		 */
		private void updateData(long[] datas, long newData) {	
			int i = 0;
			for (; i < datas.length - 1; i++) {
				datas[i] = datas[i + 1];
			}
			datas[i] = newData;
		}

	}

}
