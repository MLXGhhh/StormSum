import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;



/**
 * 计算和的Bolt节点
 */
public class SumBolt extends BaseRichBolt {

    private Map conf;   // 当前组件配置信息
    private TopologyContext context;    // 当前组件上下文对象
    private OutputCollector collector; // 发送tuple的组件

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
    }

    private Long sumOrderCost = 0L;

    /**
     * 处理数据的核心方法
     */
    public void execute(Tuple input) {
        Long orderCost = input.getLongByField("order_cost");
        sumOrderCost += orderCost;
        System.out.println("商城网站到目前" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())  + "的商品总交易额" + sumOrderCost);
        Utils.sleep(1000);
    }

    /**
     * 如果当前bolt为最后一个处理单元，该方法可以不用管
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}