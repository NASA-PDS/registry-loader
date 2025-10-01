package tt;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

public class TestLogConfigurator
{
    public static void configureLogger() 
    {
        // Configure Log4j
        ConfigurationBuilder<BuiltConfiguration> cfg = ConfigurationBuilderFactory.newConfigurationBuilder();
        cfg.setStatusLevel(Level.ERROR);
        cfg.setConfigurationName("Test");
        
        // Appenders
        addConsoleAppender(cfg, "console");

        // Root logger
        RootLoggerComponentBuilder rootLog = cfg.newRootLogger(Level.OFF);
        rootLog.add(cfg.newAppenderRef("console"));
        cfg.add(rootLog);
        
        // Default Harvest logger
        LoggerComponentBuilder defLog = cfg.newLogger("gov.nasa.pds", Level.ALL);
        cfg.add(defLog);
        
        // Init Log4j
        Configurator.initialize(cfg.build());
    }

    
    private static void addConsoleAppender(ConfigurationBuilder<BuiltConfiguration> cfg, String name)
    {
        AppenderComponentBuilder appender = cfg.newAppender(name, "CONSOLE");
        appender.addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
        appender.add(cfg.newLayout("PatternLayout").addAttribute("pattern", "[%level] %msg%n%throwable"));
        cfg.add(appender);
    }

}
