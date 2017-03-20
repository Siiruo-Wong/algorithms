/*
 * @Copyright (c) 2017 Nanjing University Of Posts And Telecommunications (NUPT).  All rights reserved.
 */
package edu.nupt.loadbalancing.beans;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Project: loadbalancing
 * @Description: The entity class is mainly for the convenience of the
 *               implementation of the Yen K-Top shortest path algorithm, and
 *               similar to {@link org.opendaylight.controller.sal.core.Path},
 *               the class's function attribute is only edges list,while
 *               provides some convenient operation.
 * @Author: Yanjun Wang
 * @Date: 2017年3月17日
 */
public class NuptPath implements Serializable {

	private static final long serialVersionUID = -3440647827570577550L;
	protected static final Logger logger = LoggerFactory.getLogger(NuptPath.class);
	private List<Edge> edges;

	/**
	 * Creates a path instance based on a given list of edges
	 * 
	 * @param edges
	 *            an edge list which is prepared to be added into the path
	 * @throws ConstructionException
	 *             throws ConstructionException if validates fails
	 */
	public NuptPath(List<Edge> edges) throws ConstructionException {
		if (validate(edges)) {
			this.edges = new LinkedList<Edge>();
			for (Edge e : edges) {
				this.edges.add(e);
			}
		} else {
			System.out.println(edges);
			throw new ConstructionException(
					"the link list does not satisfy the continuity and consistency constraints.");
		}

	}

	/**
	 * Constructor overload: default
	 * 
	 * @throws ConstructionException
	 *             throws ConstructionException if validates fails
	 */
	public NuptPath() throws ConstructionException {
		this.edges = new LinkedList<Edge>();
	}

	/**
	 * Constructor overload: creates a path instance based on a given instance,
	 * and copy all edges into the new instance.
	 * 
	 * @param path
	 *            an original path which is a given instance
	 * @throws ConstructionException
	 *             throws ConstructionException if validates fails
	 */
	public NuptPath(NuptPath path) throws ConstructionException {
		this();
		if (path == null) {
			throw new ConstructionException("parameter path is null.");
		}

		for (Edge e : path.edges) {
			this.edges.add(e);
		}
	}

	/**
	 * The method takes mainly responsibility for validating the continuity and
	 * consistency of an instance of {@link NuptPath}. The connection about HEAD
	 * and TAIL is a prerequisite for continuity of a path, so it must check the
	 * joint situation for all path sections. And,once there is a discontinuity,
	 * the verification fails and directly returns FALSE, otherwise success and
	 * returns TRUE. Of course, if the parameter {#edges} is not NULL but is
	 * empty, returns TRUE, because the path is legal just no any element.
	 * 
	 * @param edges
	 *            an edge list which is prepared to be validated
	 * @return boolean type which indicates the validation result
	 */
	private boolean validate(List<Edge> edges) {
		if (edges == null) {
			return false;
		}
		if (edges.isEmpty()) {
			return true;
		}
		Edge pre = edges.get(0);
		Edge cur = null;
		for (int i = 1; i < edges.size(); i++) {
			cur = edges.get(i);
			if (!pre.getHeadNodeConnector().getNode().equals(cur.getTailNodeConnector().getNode())) {
				return false;
			}
			pre = cur;
		}
		return true;
	}

	/**
	 * Gets all edges in the path.
	 * 
	 * @return edge list
	 */
	public List<Edge> getEdges() {
		return edges;
	}

	/**
	 * Gets all vertexes in the path. Note:the number is not equal to the size
	 * of the edges list. The factual situation:
	 *  v0---->v1 
	 *  	   v1---->v2 
	 *                v2---->v3
	 *                        ...... 
	 *                             ....... 
	 *                                   .---->v(n-1) 
	 *                                         v(n-1)---->v(n) 
	 * So,please remember that: the number of vertexes is equal to the number 
	 * of edges plus one. Of course, if there is no edge in the path, then
	 * the number of nodes is naturally equal to 0, because there is no 
	 * edge associated with a probable node.
	 * 
	 * @return 
	 * 			the size of vertexes in the path
	 */
	public int nodeSize() {
		return this.edges.isEmpty() ? 0 : this.edges.size() + 1;
	}

	/**
	 * The method is responsible for retrieving all vertexes from the path.
	 * Please refer to method @see {@link #nodeSize()} for the principles
	 * involved in this method.
	 * 
	 * @param index
	 *            the index of corresponding vertex
	 * @return 
	 * 			  the corresponding vertex
	 */
	public Node getNode(int index) {
		if (index < 0 || index > this.edges.size()) {
			return null;
		}
		NodeConnector connector = null;
		if (index == 0) {
			connector = this.edges.get(index).getTailNodeConnector();
		} else {
			connector = this.edges.get(index - 1).getHeadNodeConnector();
		}

		return connector.getNode();
	}

	/**
	 * Gets the corresponding edge according to the {# index}.
	 * 
	 * @param index
	 *            the index of corresponding edge
	 * @return 
	 * 			  the corresponding edge
	 */
	public Edge getEdge(int index) {
		if (index < 0 || index >= this.edges.size()) {
			return null;
		}
		return this.edges.get(index);
	}

	/**
	 * Gets all vertexes in the path.Please refer to method @see
	 * {@link #nodeSize()} for the principles involved in this method.
	 * 
	 * @return 
	 * 			a list which storages all vertexes
	 */
	public List<Node> getNodes() {
		LinkedList<Node> nodes = new LinkedList<Node>();
		if (size() == 0)
			return nodes;
		int i = 0;
		for (; i < this.edges.size(); i++) {
			nodes.add(this.edges.get(i).getTailNodeConnector().getNode());
		}
		nodes.add(this.edges.get(i - 1).getHeadNodeConnector().getNode());
		return nodes;
	}

	/**
	 * Gets the first vertex from the path. If there is no any edge in the path,
	 * returns directly NULL.
	 * 
	 * @return the head vertex in the path
	 */
	public Node firstNode() {
		if (this.edges.isEmpty()) {
			return null;
		}
		return this.edges.get(0).getTailNodeConnector().getNode();
	}

	/**
	 * Gets the last vertex from the path. If there is no any edge in the path,
	 * returns directly NULL.
	 * 
	 * @return 
	 * 			the tail vertex in the path
	 */
	public Node lastNode() {
		if (this.edges.isEmpty()) {
			return null;
		}
		return this.edges.get(size() - 1).getHeadNodeConnector().getNode();
	}

	/**
	 * Get the size of edges in the path.And,the size can represent the length
	 * of current path.
	 * 
	 * @return 
	 * 			the length of the path
	 */
	public int size() {
		return this.edges.size();
	}

	/**
	 * The method is used to obtain a valid sub-path based on the
	 * <tt>starting {#start}</tt> and <tt>terminating {#end}</tt> node index
	 * (rather than the edge index) in a specified and full path.Unlike the Java
	 * collection class/interface such as {@link List}, the subroutine returned
	 * by this method will contain the starting node and the terminating
	 * node.According to the analysis of method @see {@link #nodeSize()}, the
	 * <tt>starting {#start}</tt> index must be non-negative, and less than or
	 * equal to the <tt>
	 * terminating {#end}</tt> index, and the terminating index can not be
	 * greater than the number of edges, otherwise returns directly NULL.
	 * 
	 * @param start
	 *            the first vertex index of the sub-path
	 * @param end
	 *            the last vertex index of the sub-path
	 * @return 
	 * 			  a sub-path retrieved from current path
	 */
	public NuptPath getSubPathByNodeIndex(int start, int end) {
		int size = size();
		if (size == 0) {
			return null;
		}
		if (start > end) {
			return null;
		}
		if (start < 0 && end > size) {
			return null;
		}

		List<Edge> subPath = this.edges.subList(start, end);
		NuptPath path = null;
		try {
			path = new NuptPath(subPath);
		} catch (ConstructionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return path;
	}

	/**
	 * The method is used to append a new edge to the end of the current path.
	 * If the length of current path is equal to 0, the edge is added directly
	 * to the current path without continuity checking, but if the current path
	 * already contains other edges, a continuity check must be made. The check
	 * is whether the last node in the current path is the same as the TAIL node
	 * on the new edge, and if true, the operation successes and returns TRUE,
	 * or fails and return FALSE.The principle of the check is the same as that
	 * of @see {@link #validate(List)}.
	 * 
	 * @param edge
	 *            an edge which is prepared to be added into the last position
	 *            of the path
	 * @return 
	 * 			  boolean type which indicates the operation result
	 */
	public boolean append(Edge edge) {
		if (edge == null) {
			return false;
		}
		if (size() == 0) {
			this.edges.add(edge);
			return true;
		}

		if (this.lastNode() == edge.getTailNodeConnector().getNode()) {
			this.edges.add(edge);
			return true;
		}

		return false;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((edges == null) ? 0 : edges.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NuptPath other = (NuptPath) obj;
		if (edges == null) {
			if (other.edges != null)
				return false;
		} else if (!edges.equals(other.edges))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (int i = 0; i < this.edges.size(); i++) {
			if (i != 0) {
				// add the comma to the previous element
				sb.append(",");
			}
			sb.append(this.edges.get(i).toString());
		}
		sb.append("]");
		return sb.toString();
	}

}
