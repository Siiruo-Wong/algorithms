/*
 * @Copyright (c) 2017 Nanjing University Of Posts And Telecommunications (NUPT).  All rights reserved.
 */
package edu.nupt.loadbalancing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.opendaylight.controller.sal.core.Bandwidth;
import org.opendaylight.controller.sal.core.ConstructionException;
import org.opendaylight.controller.sal.core.Edge;
import org.opendaylight.controller.sal.core.Node;
import org.opendaylight.controller.sal.core.NodeConnector;
import org.opendaylight.controller.sal.core.Path;
import org.opendaylight.controller.sal.core.Property;
import org.opendaylight.controller.sal.core.UpdateType;
import org.opendaylight.controller.sal.topology.TopoEdgeUpdate;
import org.opendaylight.controller.sal.utils.NodeConnectorCreator;
import org.opendaylight.controller.sal.utils.NodeCreator;

import edu.nupt.loadbalancing.controls.LoadBalancingImplementation;

/**
 * 
 *@Project: loadbalancing
 *@Description:
 *				JUnit Test
 *@Author: Yanjun Wang
 *@Date: 2017年3月17日
 */
public class LoadBalancingTest {

	// protected static final Logger logger = LoggerFactory.getLogger(LoadBalancingImplementation.class);
	    @Test
	    public void testLoadBalancingAlgorithm() {
	    	LoadBalancingImplementation imp = new LoadBalancingImplementation();
	        imp.init();
	        Node node1 = NodeCreator.createOFNode((long) 1);
	        Node node2 = NodeCreator.createOFNode((long) 2);
	        Node node3 = NodeCreator.createOFNode((long) 3);
	        Node node4 = NodeCreator.createOFNode((long) 4);
	        Node node5 = NodeCreator.createOFNode((long) 5);
	        Node node6 = NodeCreator.createOFNode((long) 6);
	        List<TopoEdgeUpdate> topoedgeupdateList = new ArrayList<TopoEdgeUpdate>();
	        
	        //1
	        NodeConnector nc11 = NodeConnectorCreator.createOFNodeConnector(
	                (short) 1, node1);
	        NodeConnector nc21 = NodeConnectorCreator.createOFNodeConnector(
	                (short) 1, node2);
	        Edge edge1 = null;
	        try {
	            edge1 = new Edge(nc11, nc21);
	        } catch (ConstructionException e) {
	           e.printStackTrace();
	        }
	        Set<Property> props = new HashSet<Property>();
	        props.add(new Bandwidth(0));
	        TopoEdgeUpdate teu1 = new TopoEdgeUpdate(edge1, props, UpdateType.ADDED);
	        topoedgeupdateList.add(teu1);

	        //2
	        NodeConnector nc22 = NodeConnectorCreator.createOFNodeConnector(
	                (short) 2, node2);
	        NodeConnector nc31 = NodeConnectorCreator.createOFNodeConnector(
	                (short) 1, node3);
	        Edge edge2 = null;
	        try {
	            edge2 = new Edge(nc22, nc31);
	        } catch (ConstructionException e) {
	            e.printStackTrace();
	        }
	        Set<Property> props2 = new HashSet<Property>();
	        props2.add(new Bandwidth(0));
	        TopoEdgeUpdate teu2 = new TopoEdgeUpdate(edge2, props2,
	                UpdateType.ADDED);
	        topoedgeupdateList.add(teu2);
	        
	        //3
	        NodeConnector nc12 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 2, node1);
	        NodeConnector nc41 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 1, node4);
	        Edge edge3 = null;
	        try {
	        	edge3 = new Edge(nc12, nc41);
	        } catch (ConstructionException e) {
	        	e.printStackTrace();
	        }
	        Set<Property> props3 = new HashSet<Property>();
	        props3.add(new Bandwidth(0));
	        TopoEdgeUpdate teu3 = new TopoEdgeUpdate(edge3, props3,
	        		UpdateType.ADDED);
	        topoedgeupdateList.add(teu3);
	        
	        //4
	        NodeConnector nc42 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 2, node4);
	        NodeConnector nc23 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 3, node2);
	        Edge edge4 = null;
	        try {
	        	edge4 = new Edge(nc42, nc23);
	        } catch (ConstructionException e) {
	        	e.printStackTrace();
	        }
	        Set<Property> props4 = new HashSet<Property>();
	        props4.add(new Bandwidth(0));
	        TopoEdgeUpdate teu4 = new TopoEdgeUpdate(edge4, props4,
	        		UpdateType.ADDED);
	        topoedgeupdateList.add(teu4);
	        
	        //5
	        NodeConnector nc43 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 3, node4);
	        NodeConnector nc32 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 2, node3);
	        Edge edge5 = null;
	        try {
	        	edge5 = new Edge(nc43, nc32);
	        } catch (ConstructionException e) {
	        	e.printStackTrace();
	        }
	        Set<Property> props5 = new HashSet<Property>();
	        props5.add(new Bandwidth(0));
	        TopoEdgeUpdate teu5 = new TopoEdgeUpdate(edge5, props5,
	        		UpdateType.ADDED);
	        topoedgeupdateList.add(teu5);
	        
	        //6
	        NodeConnector nc44 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 4, node4);
	        NodeConnector nc51 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 1, node5);
	        Edge edge6 = null;
	        try {
	        	edge6 = new Edge(nc44, nc51);
	        } catch (ConstructionException e) {
	        	e.printStackTrace();
	        }
	        Set<Property> props6 = new HashSet<Property>();
	        props6.add(new Bandwidth(0));
	        TopoEdgeUpdate teu6 = new TopoEdgeUpdate(edge6, props6,
	        		UpdateType.ADDED);
	        topoedgeupdateList.add(teu6);
	        
	        //7
	        NodeConnector nc33 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 3, node3);
	        NodeConnector nc52 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 2, node5);
	        Edge edge7 = null;
	        try {
	        	edge7 = new Edge(nc33, nc52);
	        } catch (ConstructionException e) {
	        	e.printStackTrace();
	        }
	        Set<Property> props7 = new HashSet<Property>();
	        props7.add(new Bandwidth(0));
	        TopoEdgeUpdate teu7 = new TopoEdgeUpdate(edge7, props7,
	        		UpdateType.ADDED);
	        topoedgeupdateList.add(teu7);
	        
	        //8
	        NodeConnector nc34 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 4, node3);
	        NodeConnector nc61 = NodeConnectorCreator.createOFNodeConnector(
	        		(short) 1, node6);
	        Edge edge8 = null;
	        try {
	        	edge8 = new Edge(nc34, nc61);
	        } catch (ConstructionException e) {
	        	e.printStackTrace();
	        }
	        Set<Property> props8 = new HashSet<Property>();
	        props8.add(new Bandwidth(0));
	        TopoEdgeUpdate teu8 = new TopoEdgeUpdate(edge8, props8,
	        		UpdateType.ADDED);
	        topoedgeupdateList.add(teu8);
	        
	        //compute
	        imp.edgeUpdate(topoedgeupdateList);
	        imp.printGraph();
	        
	        /*
	         * test remove node:
	         * remove the node and all corresponding edges
	         */
//	        imp.removeNode(node2);
//	        imp.printGraph();
	        
	        /*
	         * test remove edge:
	         * just remove the edge
	         */
//	        imp.removeEdge(edge6);
//	        imp.printGraph();

	        
	        /*
	         * test add node:
	         * just add the node
	         */
//	        imp.addNode(node6);
//	        imp.printGraph();
	        
	        /*
	         * test add edge:
	         * if the node attached to the edge does not exist in the graph, the node will be added to the graph before adding the edge
	         * otherwise, just add the edge
	         */
	        
//	        imp.addEdge(edge8);
//	        imp.printGraph();
	        
	       Path res = imp.getRoute(node1, node5);

//	        List<Edge> expectedPath = (List<Edge>) new LinkedList<Edge>();
//	        expectedPath.add(0, edge1);
//	        expectedPath.add(1, edge2);
//	        Path expectedRes = null;
//	        try {
//	            expectedRes = new Path(expectedPath);
//	        } catch (ConstructionException e) {
//	           e.printStackTrace();
//	        }
//	       // if (!res.equals(expectedRes)) {
	            System.out.println("Actual Res is " + res);
	           // System.out.println("Expected Res is " + expectedRes);
	        //}
	     //   Assert.assertTrue(res.equals(expectedRes));
	    }
	
}
