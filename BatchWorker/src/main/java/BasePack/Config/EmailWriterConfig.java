package BasePack.Config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;

@Configuration
public class EmailWriterConfig {

    @Bean
    public DirectChannelSpec emailOutboundChannel() {
        return MessageChannels.direct();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setChannelTransacted(true);
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (ack) {
                System.out.println("Message sent successfully");
            } else {
                System.err.println("Failed to send message: " + cause);
            }
        });
        return rabbitTemplate;
    }

    @Bean
    public IntegrationFlow emailOutboundFlow(RabbitTemplate rabbitTemplate) {
        return IntegrationFlow
            .from(emailOutboundChannel())
                .handle(Amqp.outboundAdapter(rabbitTemplate).routingKey("emailQueue")
            ).get();
    }

}
