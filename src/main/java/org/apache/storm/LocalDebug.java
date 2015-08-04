package org.apache.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Properties;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.apache.pig.tools.grunt.GruntStorm;
import org.apache.pig.tools.timer.PerformanceTimerFactory;

public class LocalDebug {

  private final static Log log = LogFactory.getLog(LocalDebug.class);

  public static void main(String args[]) {
    System.exit(run(args));
  }

  static int run(String args[]) {
    int rc = 1;
    boolean gruntCalled = false;
    try {
      Properties properties = new Properties();
      //
      PropertiesUtil.loadDefaultProperties(properties);

      CmdLineParser opts = new CmdLineParser(args);
      opts.registerOpt('x', "exectype", CmdLineParser.ValueExpected.REQUIRED);

      ExecType execType = ExecType.LOCAL;
      UDFContext.getUDFContext().setClientSystemProps(properties);
      PigContext pigContext = new PigContext(execType, properties);

      ConsoleReader reader = new ConsoleReader(System.in,
          new OutputStreamWriter(System.out));
      reader.setDefaultPrompt("grunt> ");
      ConsoleReaderInputStream inputStream = new ConsoleReaderInputStream(
          reader);

      GruntStorm grunt = new GruntStorm(new BufferedReader(
          new InputStreamReader(inputStream)), pigContext);
      grunt.setConsoleReader(reader);
      gruntCalled = true;
      grunt.run();
      return ReturnCode.SUCCESS;

    } catch (IOException e) {
      if (e instanceof PigException) {
        PigException pe = (PigException) e;
        rc = (pe.retriable()) ? ReturnCode.RETRIABLE_EXCEPTION
            : ReturnCode.PIG_EXCEPTION;
      } else {
        rc = ReturnCode.IO_EXCEPTION;
      }

      if (!gruntCalled) {
        log.error("Error before Pig is launched", e);
      }
    } catch (Throwable e) {
      rc = ReturnCode.THROWABLE_EXCEPTION;
      if (!gruntCalled) {
        log.error("Error before Pig is launched", e);
      }
    } finally {
      // clear temp files
      FileLocalizer.deleteTempFiles();
      PerformanceTimerFactory.getPerfTimerFactory().dumpTimers();
    }

    return rc;
  }

  public static void usage() {
    System.out
        .println("USAGE: Pig [options] [-] : Run interactively in grunt shell.");

    System.out
        .println("       Pig [options] [-f[ile]] file : Run cmds found in file.");
    System.out.println("  options include:");

    System.out
        .println("    -x, -exectype - Set execution mode: local|mapreduce, default is mapreduce.");

  }

}
