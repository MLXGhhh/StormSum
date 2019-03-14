import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * 1°、实现数字累加求和的案例：数据源不断产生递增数字，对产生的数字累加求和。
 * <p>
 * Storm组件：Spout、Bolt、数据是Tuple，使用main中的Topology将spout和bolt进行关联
 * MapReduce的组件：Mapper和Reducer、数据是Writable，通过一个main中的job将二者关联
 * <p>
 * 适配器模式（Adapter）：BaseRichSpout，其对继承接口中一些没必要的方法进行了重写，但其重写的代码没有实现任何功能。
 *                        我们称这为适配器模式
 */
public class StormLocalSumTopology {
    /**
     * 构建拓扑，相当于在MapReduce中构建Job
     */
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        /**
         * 设置spout和bolt的dag（有向无环图）
         */
        builder.setSpout("id_order_spout", new OrderSpout());
        builder.setBolt("id_sum_bolt", new SumBolt())
                .shuffleGrouping("id_order_spout"); // 通过不同的数据流转方式，来指定数据的上游组件
        // 使用builder构建topology
        StormTopology topology = builder.createTopology();
        // 启动topology
        LocalCluster localCluster = new LocalCluster(); // 本地开发模式，创建的对象为LocalCluster
        String topologyName = StormLocalSumTopology.class.getSimpleName();  // 拓扑的名称
        Config config = new Config();   // Config()对象继承自HashMap，但本身封装了一些基本的配置
        localCluster.submitTopology(topologyName, config, topology);
    }
}