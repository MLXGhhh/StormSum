import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * 数据源
 * */

public class OrderSpout extends BaseRichSpout {

    private Map conf;   // 当前组件配置信息
    private TopologyContext context;    // 当前组件上下文对象
    private SpoutOutputCollector collector; // 发送tuple的组件

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.context = context;
        this.collector = collector;
    }

    /**
     * 接收数据的核心方法
     */
    public void nextTuple() {
        long num = 0;
        while (true) {
            num++;
            Utils.sleep(2000);
            System.out.println("当前时间" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "产生的订单金额：" + num);
            this.collector.emit(new Values(num));
        }
    }

    /**
     * 是对发送出去的数据的描述schema
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("order_cost"));
    }
}
