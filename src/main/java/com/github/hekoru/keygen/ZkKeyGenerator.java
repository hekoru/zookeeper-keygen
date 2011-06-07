package com.github.hekoru.keygen;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Very simple implementation of a sequential key generator using ZooKeeper. It uses node versions to adquire blocks
 * of ids.
 *
 * User: hector
 * Date: 20/04/11
 * Time: 11:10
 *
 */
public class ZkKeyGenerator {

    /**
     * Zookeeper instance
     */
    private final ZooKeeper zk;

    /**
     * Configured node id
     */
    private final String nodeId;

    /**
     * Node id in bytes
     */
    private final byte[] nodeIdBytes;

    /**
     * Map containing state of each resource
     */
    Map<String, ResourceStatus> resourceStatusMap = new HashMap<String, ResourceStatus>();

    /**
     * Log
     */
    private final Logger logger = Logger.getLogger(ZkKeyGenerator.class);

    /**
     * Where the buckets are stored
     */
    private static final String KEY_BUCKET_PATH = "/keygen/buckets/%s";

    /**
     * What resources there are and their bucket config
     */
    private static final String RESOURCES_CONFIG_PATH = "/keygen/resources";


    public ZkKeyGenerator(ZooKeeper zk, String processId, int timeout) throws Exception {
        this.zk = zk;
        this.nodeId = processId + "#" + InetAddress.getLocalHost().getHostName();
        this.nodeIdBytes = nodeId.getBytes();

        loadCfgFromZK();
    }


    /**
     * Load configuration from zookeeper
     *
     * @throws Exception
     */
    private void loadCfgFromZK() throws Exception {
        List<String> configuredResource = zk.getChildren(RESOURCES_CONFIG_PATH, false);
        if (configuredResource.isEmpty()) {
            throw new RuntimeException("No resources configured!");
        }

        logger.info("Available resources: " + configuredResource);

        for (String resource : configuredResource) {

            int bucketSize = Integer.parseInt(new String(zk.getData(RESOURCES_CONFIG_PATH + "/" + resource, false, new Stat())));

            ResourceStatus status = new ResourceStatus();
            status.resourceName = resource;
            status.bucketSize = bucketSize;

            resourceStatusMap.put(resource, status);

            logger.info("Resource " + resource + ", bucket size: " + bucketSize);
        }
    }


    /**
     *  Return a new id
     *
     * @param resource
     * @return
     * @throws Exception
     */
    public long getId(String resource) throws Exception {
        ResourceStatus rs = resourceStatusMap.get(resource);

        if (rs == null)
            throw new IllegalArgumentException("Not configured resource! " + resource);

        //Block if somebody is asking for an id from the same resource
        synchronized (rs) {

            if (rs.availableIds == 0) {

                if(logger.isDebugEnabled())
                    logger.debug("No more available ids for resource " + resource);

                int bucket = getNewBucket(resource);
                rs.nextId = bucket * rs.bucketSize;
                rs.availableIds = rs.bucketSize;
            }

            rs.availableIds--;
            return rs.nextId++;
        }

    }


    private static class ResourceStatus {
        private String resourceName;
        private int bucketSize;

        private int availableIds;
        private long nextId;
    }

    public int getNewBucket(String resource) throws Exception {

        String path = String.format(KEY_BUCKET_PATH, resource);

        boolean succesfull = false;
        int bucket = 0;

        //Two part operation: Frist retrieve the latest version id for the record and try to update against that version
        //If it fails try again.

        while(!succesfull) {
            Stat stat = new Stat();
            byte[] currentBucketBytes = zk.getData(path,false,stat);
            bucket = (currentBucketBytes != null) ? Integer.parseInt(new String(currentBucketBytes)) : 0;

            //Increment bucket counter and try to set it with the obtained version
            bucket++;

            try {
                zk.setData(path, Integer.toString(bucket).getBytes(), stat.getVersion());
                succesfull = true;
            } catch(KeeperException.BadVersionException bve) {
                if(logger.isDebugEnabled())
                    logger.debug("Somebody stepped on us! Retry!");
            }

        }



        return bucket;
    }


}
