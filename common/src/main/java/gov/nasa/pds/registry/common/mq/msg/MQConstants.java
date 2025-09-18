package gov.nasa.pds.registry.common.mq.msg;

/**
 * Message queue constants, such as queue names.
 * @author karpenko
 */
public interface MQConstants
{
    // Harvest message queues
    public static final String MQ_JOBS = "harvest.jobs";
    public static final String MQ_DIRS = "harvest.dirs";
    public static final String MQ_PRODUCTS = "harvest.products";
    public static final String MQ_COLLECTIONS = "harvest.collections";
    public static final String MQ_COLLECTION_INVENTORY = "harvest.collections";

    // Manager message queues
    public static final String MQ_MANAGER_COMMANDS = "manager.commands";
}
