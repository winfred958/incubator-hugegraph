/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.job.algorithm.comm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.iterator.ListIterator;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.job.algorithm.AbstractAlgorithm;
import com.baidu.hugegraph.job.algorithm.AbstractAlgorithm.AlgoTraverser;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;

public class LouvainTraverser extends AlgoTraverser {

    public static final String C_PASS = "c_pass-";
    public static final String C_KIN = "c_kin";
    public static final String C_WEIGHT = "c_weight";
    public static final String C_MEMBERS = "c_members";

    public static final String C_LABEL = LpaAlgorithm.Traverser.C_LABEL;

    private static final long LIMIT = AbstractAlgorithm.MAX_QUERY_LIMIT;

    private static final Logger LOG = Log.logger(LouvainTraverser.class);

    private final GraphTraversalSource g;
    private final long m;
    private final String sourceLabel;
    private final String sourceCLabel;
    private final long degree;
    private final Cache cache;

    private String passLabel;

    public LouvainTraverser(Job<Object> job,  long degree,
                            String sourceLabel, String sourceCLabel) {
        super(job);
        this.g = this.graph().traversal();
        this.m = this.g.E().count().next();
        this.sourceLabel = sourceLabel;
        this.sourceCLabel = sourceCLabel;
        this.degree = degree;
        this.passLabel = "";

        this.cache = new Cache();
    }

    @SuppressWarnings("unused")
    private Id genId2(int pass, Id cid) {
        // gen id for merge-community vertex
        String id = cid.toString();
        if (pass == 0) {
            // conncat pass with cid
            id = pass + "~" + id;
        } else {
            // replace last pass with current pass
            String lastPass = String.valueOf(pass - 1);
            assert id.startsWith(lastPass);
            id = id.substring(lastPass.length());
            id = pass + id;
        }
        return IdGenerator.of(id);
    }

    private void defineSchemaOfPk() {
        String label = this.labelOfPassN(0);
        if (this.graph().existsVertexLabel(label) ||
            this.graph().existsEdgeLabel(label)) {
            throw new IllegalArgumentException(
                      "Please clear historical results before proceeding");
        }

        SchemaManager schema = this.graph().schema();
        schema.propertyKey(C_KIN).asInt()
              .ifNotExist().create();
        schema.propertyKey(C_MEMBERS).valueSet().asText()
              .ifNotExist().create();
        schema.propertyKey(C_WEIGHT).asFloat()
              .ifNotExist().create();
    }

    private void defineSchemaOfPassN(int pass) {
        this.passLabel = labelOfPassN(pass);

        SchemaManager schema = this.graph().schema();
        try {
            schema.vertexLabel(this.passLabel).useCustomizeStringId()
                  .properties(C_KIN, C_MEMBERS)
                  .nullableKeys(C_KIN, C_MEMBERS)
                  .create();
            schema.edgeLabel(this.passLabel)
                  .sourceLabel(this.passLabel)
                  .targetLabel(this.passLabel)
                  .properties(C_WEIGHT)
                  .create();
        } catch (ExistedException e) {
            throw new IllegalArgumentException(
                      "Please clear historical results before proceeding", e);
        }
    }

    private List<String> cpassEdgeLabels() {
        List<String> names = new ArrayList<>();
        for (SchemaLabel label : this.graph().schema().getEdgeLabels()) {
            String name = label.name();
            if (name.startsWith(C_PASS)) {
                names.add(name);
            }
        }
        return names;
    }

    private List<String> cpassVertexLabels() {
        List<String> names = new ArrayList<>();
        for (SchemaLabel label : this.graph().schema().getVertexLabels()) {
            String name = label.name();
            if (name.startsWith(C_PASS)) {
                names.add(name);
            }
        }
        return names;
    }

    private String labelOfPassN(int n) {
        return C_PASS + n;
    }

    private float weightOfEdge(Edge e) {
        if (e.label().startsWith(C_PASS)) {
            assert e.property(C_WEIGHT).isPresent();
            return e.value(C_WEIGHT);
        } else if (e.property(C_WEIGHT).isPresent()) {
            return e.value(C_WEIGHT);
        }
        return 1f;
    }

    private float weightOfEdges(List<Edge> edges) {
        float weight = 0f;
        for (Edge edge : edges) {
            weight += weightOfEdge(edge);
        }
        return weight;
    }

    private Vertex newCommunityNode(Id cid, int kin, List<String> members) {
        assert !members.isEmpty() : members;
        return this.graph().addVertex(T.label, this.passLabel, T.id, cid,
                                      C_KIN, kin, C_MEMBERS, members);
    }

    private Vertex makeCommunityNode(Id cid) {
        VertexLabel vl = this.graph().vertexLabel(this.passLabel);
        return new HugeVertex(this.graph(), cid, vl);
    }

    private Edge newCommunityEdge(Vertex source, Vertex target, float weight) {
        return source.addEdge(this.passLabel, target, C_WEIGHT, weight);
    }

    private void insertNewCommunity(int pass, Id cid, int kin,
                                    List<String> members,
                                    Map<Id, MutableInt> cedges) {
        // create backend vertex if it's the first time
        Id vid = this.cache.genId(pass, cid);
        Vertex node = this.newCommunityNode(vid, kin, members);
        commitIfNeeded();
        // update backend vertex edges
        for (Map.Entry<Id, MutableInt> e : cedges.entrySet()) {
            float weight = e.getValue().floatValue();
            vid = this.cache.genId(pass, e.getKey());
            Vertex targetV = this.makeCommunityNode(vid);
            this.newCommunityEdge(node, targetV, weight);
            commitIfNeeded();
        }
        LOG.debug("Add new comm: {} kin={} size={}", node, kin, members.size());
    }

    private boolean needSkipVertex(int pass, Vertex v) {
        // skip the old intermediate data when first pass
        String label = v.label();
        if (label.startsWith(C_PASS)) {
            if (pass == 0) {
                return true;
            }
            String lastPassLabel = labelOfPassN(pass - 1);
            if (!label.equals(lastPassLabel)) {
                return true;
            }
        }
        // skip the vertex with unmatched clabel
        if (this.sourceCLabel != null &&
            !match(v, C_LABEL, this.sourceCLabel)) {
            return true;
        }
        return false;
    }

    private Iterator<Vertex> sourceVertices(int pass) {
        if (pass > 0) {
            // all vertices of merged community
            String lastPassLabel = labelOfPassN(pass - 1);
            return this.vertices(lastPassLabel, LIMIT);
        } else {
            assert pass == 0;
            // all vertices at the first time
            return this.vertices(this.sourceLabel, LIMIT);
        }
    }

    private List<Edge> neighbors(Id vid) {
        Iterator<Edge> nbs = this.edgesOfVertex(vid, Directions.BOTH,
                                                (Id) null, this.degree);
        @SuppressWarnings("resource")
        ListIterator<Edge> list = new ListIterator<>(LIMIT, nbs);
        return (List<Edge>) list.list();
    }

    private float weightOfVertex(Vertex v, List<Edge> edges) {
        Float value = this.cache.vertexWeight((Id) v.id());
        if (value != null) {
            return value;
        }
        if (edges == null) {
            edges = neighbors((Id) v.id());
        }
        float weight = weightOfEdges(edges);
        this.cache.vertexWeight((Id) v.id(), weight);
        return weight;
    }

    private int kinOfVertex(Vertex v) {
        if (v.label().startsWith(C_PASS) && v.property(C_KIN).isPresent()) {
            return v.value(C_KIN);
        }
        return 0;
    }

    private Id cidOfVertex(Vertex v) {
        Id vid = (Id) v.id();
        Community c = this.cache.vertex2Community(vid);
        return c != null ? c.cid : vid;
    }

    // 1: wrap original vertex as community node
    // 2: add original vertices to community node,
    //    and save as community vertex when merge()
    // 3: wrap community vertex as community node,
    //    and repeat step 2 and step 3.
    private Community wrapCommunity(Vertex otherV) {
        Id vid = (Id) otherV.id();
        Community comm = this.cache.vertex2Community(vid);
        if (comm != null) {
            return comm;
        }

        comm = new Community(vid);
        comm.add(this, otherV, null); // will traverse the neighbors of otherV
        this.cache.vertex2Community(vid, comm);
        return comm;
    }

    private Collection<Pair<Community, MutableInt>> nbCommunities(
                                                    int pass,
                                                    List<Edge> edges) {
        // comms is a map of cid:[community,weight]
        Map<Id, Pair<Community, MutableInt>> comms = new HashMap<>();
        for (Edge edge : edges) {
            Vertex otherV = ((HugeEdge) edge).otherVertex();
            if (needSkipVertex(pass, otherV)) {
                // skip the old intermediate data, or filter clabel
                continue;
            }
            Community c = wrapCommunity(otherV);
            if (!comms.containsKey(c.cid)) {
                comms.put(c.cid, Pair.of(c, new MutableInt(0)));
            }
            // calc weight between source vertex and neighbor community
            comms.get(c.cid).getRight().add(2 * weightOfEdge(edge));
        }
        return comms.values();
    }

    private void moveCommunity(Vertex v, List<Edge> nbs, Community newC) {
        Id vid = (Id) v.id();

        // remove v from old community
        Community oldC = this.cache.vertex2Community(vid);
        if (oldC != null) {
            oldC.remove(this, v, nbs);
        }

        // add v to new community
        newC.add(this, v, nbs);
        LOG.debug("Move {} to comm: {}", v, newC);

        // update community of v
        this.cache.vertex2Community(vid, newC);
    }

    private double moveCommunities(int pass) {
        Iterator<Vertex> vertices = this.sourceVertices(pass);

        // shuffle
        //r = r.order().by(shuffle);

        long total = 0L;
        long moved = 0L;
        while (vertices.hasNext()) {
            this.updateProgress(++this.progress);
            Vertex v = vertices.next();
            if (needSkipVertex(pass, v)) {
                // skip the old intermediate data, or filter clabel
                continue;
            }
            total++;
            Id cid = cidOfVertex(v);
            List<Edge> nbs = neighbors((Id) v.id());
            double ki = kinOfVertex(v) + weightOfVertex(v, nbs);
            // update community of v if △Q changed
            double maxDeltaQ = 0d;
            Community bestComm = null;
            // list all neighbor communities of v
            for (Pair<Community, MutableInt> nbc : nbCommunities(pass, nbs)) {
                // △Q = (Ki_in - Ki * Etot / m) / 2m
                Community otherC = nbc.getLeft();
                // weight between c and otherC
                double kiin = nbc.getRight().floatValue();
                // weight of otherC
                int tot = otherC.kin() + otherC.kout();
                if (cid.equals(otherC.cid)) {
                    tot -= ki;
                    assert tot >= 0;
                    // expect tot >= 0, but may be something wrong?
                    if (tot < 0) {
                        tot = 0;
                    }
                }
                double deltaQ = kiin - ki * tot / this.m;
                if (deltaQ > maxDeltaQ) {
                    // TODO: cache otherC for neighbors the same community
                    maxDeltaQ = deltaQ;
                    bestComm = otherC;
                }
            }
            if (maxDeltaQ > 0d && !cid.equals(bestComm.cid)) {
                moved++;
                // move v to the community of maxQ neighbor
                moveCommunity(v, nbs, bestComm);
            }
        }

        // maybe always shocking when set degree limit
        return total == 0L ? 0d : (double) moved / total;
    }

    private void mergeCommunities(int pass) {
        // merge each community as a vertex
        Collection<Pair<Community, Set<Id>>> comms = this.cache.communities();
        this.cache.resetVertexWeight();
        for (Pair<Community, Set<Id>> pair : comms) {
            Community c = pair.getKey();
            if (c.empty()) {
                continue;
            }
            // update kin and edges between communities
            int kin = c.kin();
            Set<Id> vertices = pair.getRight();
            assert !vertices.isEmpty();
            List<String> members = new ArrayList<>(vertices.size());
            Map<Id, MutableInt> cedges = new HashMap<>(vertices.size());
            for (Id v : vertices) {
                members.add(v.toString());
                // collect edges between this community and other communities
                List<Edge> neighbors = neighbors(v);
                for (Edge edge : neighbors) {
                    Vertex otherV = ((HugeEdge) edge).otherVertex();
                    if (vertices.contains(otherV.id())) {
                        // inner edges of this community, will be calc twice
                        // due to both e-in and e-out are in vertices,
                        kin += weightOfEdge(edge);
                        continue;
                    }
                    Id otherCid = cidOfVertex(otherV);
                    if (otherCid.compareTo(c.cid) < 0) {
                        // skip if it should be collected by otherC
                        continue;
                    }
                    if (!cedges.containsKey(otherCid)) {
                        cedges.put(otherCid, new MutableInt(0));
                    }
                    cedges.get(otherCid).add(weightOfEdge(edge));
                }
            }
            // insert new community vertex and edges into storage
            this.insertNewCommunity(pass, c.cid, kin, members, cedges);
        }
        this.graph().tx().commit();
        // reset communities
        this.cache.reset();
    }

    public Object louvain(int maxTimes, int stableTimes, double precision) {
        assert maxTimes > 0;
        assert precision > 0d;

        this.defineSchemaOfPk();

        /*
         * iterate until it has stabilized or
         * the maximum number of times is reached
         */
        int times = maxTimes;
        int movedTimes = 0;
        double movedPercent = 0d;
        double lastMovedPercent = 0d;

        for (int i = 0; i < maxTimes; i++) {
            boolean finished = true;
            movedPercent = 0d;
            lastMovedPercent = 1d;
            int tinyChanges = 0;
            while ((movedPercent = this.moveCommunities(i)) > 0d) {
                movedTimes++;
                finished = false;
                if (lastMovedPercent - movedPercent < precision) {
                    tinyChanges++;
                }
                if (i == 0 && movedPercent < precision) {
                    // stop the first round of iterations early
                    break;
                }
                if (tinyChanges >= stableTimes) {
                    // maybe always shaking and falling into an dead loop
                    break;
                }
                lastMovedPercent = movedPercent;
            }
            if (finished) {
                times = i;
                break;
            } else {
                this.defineSchemaOfPassN(i);
                this.mergeCommunities(i);
            }
        }

        long communities = 0L;
        String commLabel = this.passLabel;
        if (!commLabel.isEmpty()) {
            GraphTraversal<?, Long> t = this.g.V().hasLabel(commLabel).count();
            communities = this.execute(t, t::next);
        }
        return ImmutableMap.of("pass_times", times,
                               "phase1_times", movedTimes,
                               "last_precision", movedPercent,
                               "times", maxTimes,
                               "communities", communities);
    }

    public double modularity(int pass) {
        // pass: label the last pass
        String label = labelOfPassN(pass);
        Number kin = this.g.V().hasLabel(label).values(C_KIN).sum().next();
        Number weight = this.g.E().hasLabel(label).values(C_WEIGHT).sum().next();
        double m = kin.intValue() + weight.floatValue() * 2.0d;
        double q = 0.0d;
        Iterator<Vertex> coms = this.g.V().hasLabel(label);
        while (coms.hasNext()) {
            Vertex com = coms.next();
            int cin = com.value(C_KIN);
            Number cout = this.g.V(com).bothE().values(C_WEIGHT).sum().next();
            double cdegree = cin + cout.floatValue();
            // Q = ∑(I/M - ((2I+O)/2M)^2)
            q += cin / m - Math.pow(cdegree / m, 2);
        }
        return q;
    }

    public Collection<Object> showCommunity(String community) {
        final String C_PASS0 = labelOfPassN(0);
        Collection<Object> comms = Arrays.asList(community);
        boolean reachPass0 = false;
        while (comms.size() > 0 && !reachPass0) {
            Iterator<Vertex> subComms = this.vertices(comms.iterator());
            comms = new HashSet<>();
            while (subComms.hasNext()) {
                this.updateProgress(++this.progress);
                Vertex sub = subComms.next();
                if (sub.property(C_MEMBERS).isPresent()) {
                    Set<Object> members = sub.value(C_MEMBERS);
                    reachPass0 =  sub.label().equals(C_PASS0);
                    comms.addAll(members);
                }
            }
        }
        return comms;
    }

    public long clearPass(int pass) {
        GraphTraversal<Edge, Edge> te = this.g.E();
        if (pass < 0) {
            // drop edges of all pass
            List<String> els = this.cpassEdgeLabels();
            if (els.size() > 0) {
                String first = els.remove(0);
                te = te.hasLabel(first, els.toArray(new String[els.size()]));
                this.drop(te);
            }
            // drop schema
            for (String label : this.cpassEdgeLabels()) {
                this.graph().schema().edgeLabel(label).remove();
            }
        } else {
            // drop edges of pass N
            String label = labelOfPassN(pass);
            if (this.graph().existsEdgeLabel(label)) {
                te = te.hasLabel(label);
                this.drop(te);
                // drop schema
                this.graph().schema().edgeLabel(label).remove();
            }
        }

        GraphTraversal<Vertex, Vertex> tv = this.g.V();
        if (pass < 0) {
            // drop vertices of all pass
            List<String> vls = this.cpassVertexLabels();
            if (vls.size() > 0) {
                String first = vls.remove(0);
                tv = tv.hasLabel(first, vls.toArray(new String[vls.size()]));
                this.drop(tv);
            }
            // drop schema
            for (String label : this.cpassVertexLabels()) {
                this.graph().schema().vertexLabel(label).remove();
            }
        } else {
            // drop vertices of pass N
            String label = labelOfPassN(pass);
            if (this.graph().existsVertexLabel(label)) {
                tv = tv.hasLabel(label);
                this.drop(tv);
                // drop schema
                this.graph().schema().vertexLabel(label).remove();
            }
        }

        return this.progress;
    }

    private static class Community {

        // community id (stored as a backend vertex)
        private final Id cid;
        // community members size
        private int size = 0;
        /*
         * weight of all edges in community(2X), sum of kin of new members
         *  [each is from the last pass, stored in backend vertex]
         */
        private int kin = 0;
        /*
         * weight of all edges between communities, sum of kout of new members
         * [each is last pass, calculated in real time by neighbors]
         */
        //
        private int kout = 0;

        public Community(Id cid) {
            this.cid = cid;
        }

        public boolean empty() {
            return this.size <= 0;
        }

        public void add(LouvainTraverser t, Vertex v, List<Edge> nbs) {
            this.size++;
            this.kin += t.kinOfVertex(v);
            this.kout += t.weightOfVertex(v, nbs);
        }

        public void remove(LouvainTraverser t, Vertex v, List<Edge> nbs) {
            this.size--;
            this.kin -= t.kinOfVertex(v);
            this.kout -= t.weightOfVertex(v, nbs);
        }

        public int kin() {
            return this.kin;
        }

        public int kout() {
            return this.kout;
        }

        @Override
        public String toString() {
            return String.format("[%s](size=%s kin=%s kout=%s)",
                                 this.cid , this.size, this.kin, this.kout);
        }
    }

    private static class Cache {

        private final Map<Id, Float> vertexWeightCache;
        private final Map<Id, Community> vertex2Community;
        private final Map<Id, Integer> genIds;

        public Cache() {
            this.vertexWeightCache = new HashMap<>();
            this.vertex2Community = new HashMap<>();
            this.genIds = new HashMap<>();
        }

        public Community vertex2Community(Id id) {
            return this.vertex2Community.get(id);
        }

        public void vertex2Community(Id id, Community c) {
            this.vertex2Community.put(id, c);
        }

        public Float vertexWeight(Id id) {
            return this.vertexWeightCache.get(id);
        }

        public void vertexWeight(Id id, float weight) {
            this.vertexWeightCache.put(id, weight);
        }

        public void reset() {
            this.vertexWeightCache.clear();
            this.vertex2Community.clear();
            this.genIds.clear();
        }

        public void resetVertexWeight() {
            this.vertexWeightCache.clear();
        }

        public Id genId(int pass, Id cid) {
            if (!this.genIds.containsKey(cid)) {
                this.genIds.put(cid, this.genIds.size() + 1);
            }
            String id = pass + "~" + this.genIds.get(cid);
            return IdGenerator.of(id);
        }

        public Collection<Pair<Community, Set<Id>>> communities(){
            // TODO: get communities from backend store instead of ram
            Map<Id, Pair<Community, Set<Id>>> comms = new HashMap<>();
            for (Entry<Id, Community> e : this.vertex2Community.entrySet()) {
                Community c = e.getValue();
                if (c.empty()) {
                    continue;
                }
                Pair<Community, Set<Id>> pair = comms.get(c.cid);
                if (pair == null) {
                    pair = Pair.of(c, new HashSet<>());
                    comms.put(c.cid, pair);
                }
                // collect members joined to the community [current pass]
                pair.getRight().add(e.getKey());
            }
            return comms.values();
        }
    }
}
