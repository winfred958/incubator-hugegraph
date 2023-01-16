package com.shuyun.graph.test;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.StandardHugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.optimize.Text;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.JsonUtil;
import com.shuyun.graph.utils.RegisterUtil;
import io.leopard.javahost.JavaHost;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

public class GraphTest {

    private static final Logger log = LoggerFactory.getLogger(GraphTest.class);

    @Test
    public void graphInitTest() {
        initHosts();
        final Path path = Paths.get("E:\\git_dir\\incubator-hugegraph\\huge-test\\src\\main\\resources\\graphs\\hugegraph.properties");
        RegisterUtil.registerHBase();

        final HugeGraph hugeGraph = HugeFactory.open(path.toString());
        final long t1 = System.currentTimeMillis();
        List<Shard> shards = hugeGraph.metadata(HugeType.EDGE_OUT, "splits", 1048576L);

        for (Shard shard : shards) {
            final String start = shard.start();
            final String end = shard.end();
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.scan(start, end);
            query.page(start);
            if (query.paging()) {
                query.limit(100000L);
            }
            final Iterator<Vertex> vertices = hugeGraph.vertices(query);
            while (vertices.hasNext()) {
                final Vertex vertex = vertices.next();
                System.out.println("vertexId = " + vertex.id());
                log.info("vertexId = {}", vertex.id());
            }
        }

        final long t2 = System.currentTimeMillis();
        final Vertex vertex = hugeGraph.vertex("s_200");
        final long t3 = System.currentTimeMillis();
        log.info("took: {} {}", t2 - t1, t3 - t2);
        final String label = vertex.label();
        log.info("{}", label);
        if (hugeGraph instanceof StandardHugeGraph) {
            final StandardHugeGraph standardHugeGraph = (StandardHugeGraph) hugeGraph;
            try {
                standardHugeGraph.close();
            } catch (Exception e) {
                log.error("", e);
            }
        }
    }

    public static Id checkAndParseVertexId(String idValue) {
        if (idValue == null) {
            return null;
        }
        boolean uuid = idValue.startsWith("U\"");
        if (uuid) {
            idValue = idValue.substring(1);
        }
        try {
            Object id = JsonUtil.fromJson(idValue, Object.class);
            return uuid ? Text.uuid((String) id) : HugeVertex.getIdValue(id);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                "The vertex id must be formatted as Number/String/UUID" +
                    ", but got '%s'", idValue));
        }
    }

    private void initHosts() {
        try {
            JavaHost.updateVirtualDns("slave08.spacexbigdata-qa.shuyun.com", "192.168.179.48");
            JavaHost.updateVirtualDns("slave09.spacexbigdata-qa.shuyun.com", "192.168.179.49");
            JavaHost.updateVirtualDns("slave10.spacexbigdata-qa.shuyun.com", "192.168.179.50");
            JavaHost.updateVirtualDns("slave11.spacexbigdata-qa.shuyun.com", "192.168.179.51");
            JavaHost.updateVirtualDns("slave12.spacexbigdata-qa.shuyun.com", "192.168.179.52");
            JavaHost.updateVirtualDns("slave13.spacexbigdata-qa.shuyun.com", "192.168.179.53");
            JavaHost.updateVirtualDns("slave14.spacexbigdata-qa.shuyun.com", "192.168.179.54");
            JavaHost.updateVirtualDns("slave15.spacexbigdata-qa.shuyun.com", "192.168.179.55");
            JavaHost.updateVirtualDns("kafka-pre.spacexbigdata.shuyun.com", "192.168.178.219");
            JavaHost.updateVirtualDns("atlas01.spacexbigdata.shuyun.com", "192.168.178.220");
            JavaHost.updateVirtualDns("master01.spacexbigdata-qa.shuyun.com", "192.168.172.130");
            JavaHost.updateVirtualDns("slave08", "192.168.179.48");
            JavaHost.updateVirtualDns("slave09", "192.168.179.49");
            JavaHost.updateVirtualDns("slave10", "192.168.179.50");
            JavaHost.updateVirtualDns("slave11", "192.168.179.51");
            JavaHost.updateVirtualDns("slave12", "192.168.179.52");
            JavaHost.updateVirtualDns("slave13", "192.168.179.53");
            JavaHost.updateVirtualDns("slave14", "192.168.179.54");
            JavaHost.updateVirtualDns("slave15", "192.168.179.55");
            JavaHost.updateVirtualDns("kafka-pre", "192.168.178.219");
            JavaHost.updateVirtualDns("atlas01", "192.168.178.220");
            JavaHost.updateVirtualDns("master01", "192.168.172.130");
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
