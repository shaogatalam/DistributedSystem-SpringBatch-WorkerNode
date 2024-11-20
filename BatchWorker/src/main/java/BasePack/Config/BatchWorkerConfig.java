package BasePack.Config;

import BasePack.Model.Customer;
import BasePack.Model.EmailObject;
import lombok.Data;
//import lombok.Value;
//import org.springframework.batch.core.StepExecution;
//import lombok.Value;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

@Configuration
public class BatchWorkerConfig {

        @Autowired
        AmqpTemplate amqpTemplate;

        @Autowired
        private ConnectionFactory connectionFactory;

        @Autowired
        private MessageChannel emailOutboundChannel;

        @Bean
	    DirectChannelSpec repliesChannel(){
            return MessageChannels.direct();
        }
        @Bean
	    DirectChannelSpec requestsChannel(){
            return MessageChannels.direct();
        }


        @Bean
        IntegrationFlow repliesFlow(MessageChannel repliesChannel, AmqpTemplate amqpTemplate) {
            return IntegrationFlow.from(repliesChannel).handle(Amqp.outboundAdapter(amqpTemplate).routingKey("replies")).get();
        }

        @Bean
	    IntegrationFlow requestsFlow(ConnectionFactory connectionFactory, MessageChannel requestsChannel) {
            var smc = new SimpleMessageConverter();
            smc.addAllowedListPatterns("*");
            return IntegrationFlow.from(Amqp.inboundAdapter(connectionFactory,"requests").messageConverter(smc)).channel(requestsChannel).get();
        }

        @Bean
        @StepScope
        ItemReader<Customer> jdbcItemReader(DataSource dataSource,
                                            @Value("#{stepExecutionContext['partition']}") String partition,
                                            @Value("#{stepExecutionContext['startId']}") int startId,
                                            @Value("#{stepExecutionContext['endId']}") int endId) {

            System.out.println("range start id :: "+startId);
            System.out.println("range end id :: "+endId);
            return new JdbcPagingItemReaderBuilder<Customer>()
            .dataSource(dataSource)
            .sortKeys(Map.of("id",Order.ASCENDING))
            .selectClause("SELECT *")
            .fromClause("FROM customer")
            .whereClause("WHERE id BETWEEN :startId AND :endId")
            .parameterValues(Map.of("startId", startId, "endId", endId))
            .rowMapper((rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("name"), rs.getString("email")))
            .pageSize(10)
            .name("jdbcItemReader")
            .build();
        }

        @Bean
        public ItemProcessor<Customer, EmailObject> customerProcessor() {
            return customer -> {
                String personalizedMessage = "Hello " + customer.getName() + ", this is a personalized message!";
                EmailObject emailData = new EmailObject();
                emailData.setEmail(customer.getEmail());
                emailData.setTemplate(personalizedMessage);
                return emailData;
            };
        }


        // Send finalized email content to the email sender application via emailOutboundChannel
        @Bean
        public ItemWriter<EmailObject> emailWriter() {
            return emailDataList -> {
                for (EmailObject emailData : emailDataList) {
                    //amqpTemplate.convertAndSend("emailQueue", emailData);
                    emailOutboundChannel.send(MessageBuilder.withPayload(emailData).build());
                }
            };
        }

        @Bean
        public Step workerStep(JobRepository repository,
                JobExplorer explorer,
                ItemReader<Customer> customerItemReader,
                BeanFactory beanFactory,
                PlatformTransactionManager transactionManager,
                MessageChannel requestsChannel,
                MessageChannel repliesChannel) {

            return new RemotePartitioningWorkerStepBuilder("workerStep", repository)
                    .jobExplorer(explorer)
                    .inputChannel(requestsChannel)
                    .beanFactory(beanFactory)
                    .outputChannel(repliesChannel)
                    .<Customer, EmailObject>chunk(3, transactionManager)
                    .reader(customerItemReader)
                    .processor(customerProcessor())
                    .writer(emailWriter())
                    .build();
        }

}
