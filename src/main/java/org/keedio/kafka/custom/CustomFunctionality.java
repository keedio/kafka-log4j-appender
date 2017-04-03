package org.keedio.kafka.custom;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.keedio.kafka.log4jappender.KafkaLog4jAppender;

public class CustomFunctionality extends KafkaLog4jAppender {

    private final String newLine = System.getProperty("line.separator");

    /**
     * Replaces the new line character with a specific separator
     * @param messageAsBytes the message in bytes
     * @param separator the separator
     * @return the message formatted
     */
    private String replaceNewLine(byte[] messageAsBytes, String separator) {
        String messageFromBytes = new String(messageAsBytes);

        boolean hasNewline = messageFromBytes.contains(newLine);
        String messageFormatted;
        if (hasNewline)
            messageFormatted = messageFromBytes.replace(newLine, separator);
        else
            messageFormatted = messageFromBytes;
        return messageFormatted;
    }

    /**
     * Implements the subAppend method for the appender
     * @param layout the layout
     * @param event the event
     * @return the information to be displayed in the log file
     */
    public String subAppend(Layout layout, LoggingEvent event) {
        CustomFunctionality cf = new CustomFunctionality();
        String messageFormatted = cf.replaceNewLine((byte[]) event.getMessage(), "::");
        if (layout == null) {
            return messageFormatted;
        }
        else {
            String messageLayout = layout.format(event);
            StringBuilder sb = new StringBuilder()
                    .append(messageLayout.replace(newLine, "::"))
                    .append(messageFormatted);
            return sb.toString();
        }
    }
}
