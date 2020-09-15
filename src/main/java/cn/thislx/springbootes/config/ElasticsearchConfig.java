package cn.thislx.springbootes.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

/**
 * @Configuration Used to define the configuration class, replace the xml configuration file
 */
@Configuration
public class ElasticsearchConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchConfig.class);

    /**
     * elk cluster address
     */
    @Value("${elasticsearch.ip}")
    private String hostName;

    /**
     * port
     */
    @Value("${elasticsearch.port}")
    private String port;

    /**
     * Cluster Name
     */
    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    /**
     * connection pool
     */
    @Value("${elasticsearch.pool}")
    private String poolSize;

    /**
     * Bean name default  Function name
     *
     * @return
     */
    @Bean(name = "transportClient")
    public TransportClient transportClient() {
        LOGGER.info("Elasticsearch initialization begins。。。。。");
        TransportClient transportClient = null;
        try {
            // Configuration information
            Settings esSetting = Settings.builder()
                    .put("cluster.name", clusterName) //Cluster name
                    .put("client.transport.sniff", true)//Add sniffing mechanism to find ES cluster
                    .put("thread_pool.search.size", Integer.parseInt(poolSize))//Increase the number of thread pools, temporarily set to 5
                    .build();
            //Configuration information
            transportClient = new PreBuiltTransportClient(esSetting);
            TransportAddress transportAddress = new TransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port));
            transportClient.addTransportAddresses(transportAddress);
        } catch (Exception e) {
            LOGGER.error("elasticsearch TransportClient create error!!", e);
        }
        return transportClient;
    }

}
