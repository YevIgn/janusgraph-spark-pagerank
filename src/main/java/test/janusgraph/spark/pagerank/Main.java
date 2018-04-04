
package test.janusgraph.spark.pagerank;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

@SuppressWarnings("unchecked")
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String NUMBER = "number";
    private static final String VERTICES = "vertices";
    private static final String EDGES = "edges";
    private static final String EDGE = "edge";
    private static final String PAGE_RANK = "pageRank";

    private static final Configuration OLTP_GRAPH = loadConfiguration("/janusgraph-cassandra.properties");
    private static final Configuration OLAP_GRAPH = loadConfiguration("/gremlin-spark-cassandra.properties");

    private static Configuration loadConfiguration(String resource) {
        PropertiesConfiguration config = new PropertiesConfiguration();

        try (InputStream inputStream = Main.class.getResourceAsStream(resource)) {
            config.load(inputStream);
            return config;
        } catch (IOException | ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String... args) throws Exception {
        Map<String, Collection> subGraphIds;

        // Clear db.
        try (JanusGraph graph = JanusGraphFactory.open(OLTP_GRAPH)) {
            JanusGraphFactory.drop(graph);
        }

        // Prepare data and load ids of subgraph.
        try (JanusGraph graph = JanusGraphFactory.open(OLTP_GRAPH)) {
            // Set up schema.
            JanusGraphManagement management = graph.openManagement();
            if (!management.containsPropertyKey(NUMBER)) {
                PropertyKey key = management.makePropertyKey(NUMBER).dataType(Integer.class).make();
                management.buildIndex("keyIndex", Vertex.class)
                        .addKey(key)
                        .buildCompositeIndex();
                management.commit();
            }

            // Load graph.
            GraphMLReader.build()
                    .create()
                    .readGraph(Main.class.getResourceAsStream("/graph.graphml"), graph);

            //Prepare subgraph ids.
            subGraphIds = fillSubGraphIds(graph.traversal());
        }

        // Set up Spark.
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[20]");
        Spark.create(configuration);

        // Calculate and print rankings.
        try (Graph graph = GraphFactory.open(OLAP_GRAPH)) {
            runPageRank(graph, subGraphIds);
        }
    }

    private static void runPageRank(Graph graph, Map<String, Collection> subGraphIds) {
        log.info("Start running page rank.");
        Computer computer = Computer.compute(SparkGraphComputer.class)
                .vertices(__.hasId(P.within(subGraphIds.get(VERTICES))))
                .edges(__.bothE().hasId(P.within(subGraphIds.get(EDGES))));
        GraphTraversalSource g = graph.traversal().withComputer(computer);
        StringBuilder result = new StringBuilder();

        g
                .V()
                .pageRank()
                .by(__.bothE())
                .by(PAGE_RANK)
                .order().by(PAGE_RANK, Order.decr)
                .limit(20)
                .valueMap(NUMBER, PAGE_RANK)
                .forEachRemaining(kv -> result.append(kv.toString()).append("\n"));
        result.append("V: ").append(g.V().count().next()).append("\n")
                .append("E: ").append(g.E().count().next()).append("\n");
        log.info(result.toString());
    }

    private static Map<String, Collection> fillSubGraphIds(GraphTraversalSource g) {
        return (Map) g
                .V()
                .has(NUMBER, 204984)
                .emit()
                .repeat(__.bothE().dedup().store(EDGES).by(T.id).otherV())
                .times(2)
                .dedup()
                .aggregate(VERTICES).by(T.id)
                .bothE()
                .where(P.without(EDGES)).by(T.id).by()
                .as(EDGE)
                .otherV()
                .where(P.within(VERTICES)).by(T.id).by()
                .select(EDGE)
                .store(EDGES).by(T.id)
                .cap(VERTICES, EDGES)
                .next();
    }
}