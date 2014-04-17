package com.continuuity.common.twill;

import com.continuuity.common.conf.CConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Abstract Reactor TwillRunnable class for Reactor YARN services
 */
public abstract class AbstractReactorTwillRunnable extends AbstractTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractReactorTwillRunnable.class);

  protected String name;
  private String cConfName;
  private String hConfName;
  private CountDownLatch runLatch;
  protected Configuration hConf;
  protected CConfiguration cConf;

  public AbstractReactorTwillRunnable(String name, String cConfName, String hConfName) {
    this.name = name;
    this.cConfName = cConfName;
    this.hConfName = hConfName;
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.of("cConf", cConfName, "hConf", hConfName))
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);

    runLatch = new CountDownLatch(1);
    name = context.getSpecification().getName();
    Map<String, String> configs = context.getSpecification().getConfigs();

    try {
      // Load configuration
      hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      UserGroupInformation.setConfiguration(hConf);

      cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());

      LOG.info("{} Setting host name to {}", name, context.getHost().getCanonicalHostName());
      cConf.set(getServiceAddress(), context.getHost().getCanonicalHostName());
      LOG.debug("{} Continuuity conf {}", name, cConf);
      LOG.debug("{} HBase conf {}", name, hConf);
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void run() {

    List<Service> services = getService();
    Preconditions.checkNotNull(services, "ServiceList cannot be null");
    Preconditions.checkArgument(!services.isEmpty(), "Should have atleast one service");

    LOG.info("Starting runnable {}", name);
    for (Service service : services) {
      Futures.getUnchecked(Services.chainStart(service));
    }
    LOG.info("Runnable started {}", name);

    try {
      runLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Waiting on latch interrupted {}", name);
      Thread.currentThread().interrupt();
    } finally {
      Collections.reverse(services);
      for (Service service : services) {
        Futures.getUnchecked(Services.chainStop(service));
      }
    }

    LOG.info("Runnable stopped {}", name);
  }

  @Override
  public void stop() {
    runLatch.countDown();
  }

  /**
   * Class extending AbstractReactorTwillRunnable should implement a getService method that
   * returns a list of Services which will be started in increasing order of index.
   * The services will be stopped in the reverse order
   * @return A List of Services
   */
  public abstract List<Service> getService();

  //TODO: Not the most elegant way (ie to force the subclass to implement getServiceAddress)
  /**
   * Should return the XML property in CConfiguration where the host name needs to be set
   * for the service to make use of
   * @return
   */
  public abstract String getServiceAddress();

}
