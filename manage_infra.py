"""
Python script to manage running jobs, checking the results, etc.

TODOs:
- Build the demo classpath with Maven
- Figure out all of the dependent JARs for the retail demo and copy them over to the cluster
- Include the dependent jobs in the classpath for the bulk importer

- Modify express.sh such that it echoes all of the commands that it runs.
- Build fat jar for the job?  (fix guava 15 issues)

"""



import argparse
import fabric.api
import fabric.network
import os
import sys
import re
import shutil
import textwrap
import tabulate
import time
import urllib2
from argparse import RawTextHelpFormatter

# Command-line "actions" the user can use and the corresponding help messages.
actions_help = {}

# Maintains order of possible actions for help.
possible_actions = []

def add_action(action_name, help_text=''):
  assert not actions_help.has_key(action_name)
  possible_actions.append(action_name)
  # TODO: Remove any extra random white space from the help.
  #p = re.compile(r'\s+')
  #actions_help[action_name] = p.replace(' ', action_help)
  actions_help[action_name] = textwrap.dedent(help_text)

add_action("help", "Display help about different actions.")

add_action("install-bento", """\
        Will set up a bento box for you.  Assumes that you are in a directory with a tar.gz file for the latest bento
        build. This command will rm -rf your current bento box (which is also assumed to be in your current
        directory).  Also does stuff like editing out lines that break bin/kiji and updating the scoring server conf
        file appropriately.""")

add_action("copy-bento", """\
        Will tar up the current bento box (after whatever edits were made in the previous step, copy it to the infra
        cluster, and unpack it.""")

add_action("install-kiji", "Install kiji instance for wibi retail")
add_action("create-tables", "Create the WibiRetail tables.")

add_action("install-model-repo", "Create a directory on the server for the model repo and initialize the mode repo.")
add_action("start-scoring-server", "Update the system table and start the scoring server, then verify that it is working.")

add_action("copy-bb-data-to-hdfs", "Copy the Best Buy data onto HDFS in the cluster.")

add_action('prepare-bulk-import', 'Create lib dir for bulk import job.')
add_action('bulk-import', 'Bulk import the Best Buy data to Kiji')

add_action('prepare-batch-train', 'Create lib dir for the model code.')
add_action('batch-train', 'Run training for customers also viewed, purchases (no TFIDF...)')

add_action('start-jetty')


description = """
Script to set up WibiRetail on Cassandra-Kiji on the infra cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

How it works:
- You provide pointers to your bento and retail tgz files
- The script unzips them
- It edits the bento box, zips it up again, and scps it to the cluster
- It can then set up services (e.g., model repo, scoring server on the cluster)
- It will run most of the kiji commands (e.g., install, put) from the client

"""

def format_multiline_command(cmd):
  """
  Reformat a multiline string as a shell command. (Basically remote new lines.)

  :param cmd: multiline shell command.
  :return: Shell command that is okay to actually run.
  """
  return textwrap.dedent(cmd).replace('\n', ' ')

class InfraManager:

  def _help_actions(self):
    """ Print detailed information about how the different actions work """
    actions_str = ""
    for action in possible_actions:
      help_text = actions_help[action]
      actions_str += "command: %s\n%s\n\n" % (action, help_text)
    print(actions_str)
    sys.exit(0)

  def _setup_parser(self):
    """ Add actions for the command-line arguments parser """
    parser = argparse.ArgumentParser(
      formatter_class=RawTextHelpFormatter,
      description=description + "Possible actions:\n\t" + "\n\t".join(possible_actions))

    parser.add_argument(
      "action",
      nargs='*',
      help="Action to take")

    parser.add_argument(
      '--bento-tgz',
      help='Bento box archive file name (default is bento-*-tar.gz in pwd)',
      default=None)

    parser.add_argument(
      '--demo-home',
      help='Home directory for the retail demo (assumed to be source, after running "mvn clean package"' +
           ' (Default is pwd/retail-demo)',
      default='/Users/clint/work/wibidata/retail-demo')

    parser.add_argument(
      '--retail-home',
      help='Home directory for wibi-retail-core (assumed to be source, after running "mvn clean package"' +
           ' (Default is pwd/wibi-retail-core)',
      default='/Users/clint/work/wibidata/wibi-retail-core')

    return parser


  def _get_archive_location(self, option_value, archive_prefix):
    """
    Figure out where a .tar.gz file is located.  If specified by the user, confirm it exists.  If not, find it in
    the current working directory.

    :param option_value: User-specified location for the .tar.gz file (can be None)
    :param archive_prefix: Prefix for the archive (e.g., 'kiji-bento-')
    :return: The archive path.
    """
    if option_value is not None:
      assert os.path.isfile(option_value), option_value
      return option_value
    files_this_dir = os.listdir(os.getcwd())
    candidates = set()
    for file_name in files_this_dir:
      if file_name.startswith(archive_prefix) and file_name.endswith(".tar.gz"):
        candidates.add(file_name)
    assert len(candidates) == 1, "Could not find exactly one tar.gz file starting with %s in pwd" % archive_prefix
    return list(candidates)[0]

  def _get_bento_dir_from_archive_name(self, bento_tgz):
    bento_root = os.path.basename(bento_tgz)
    p_bento = re.compile(r'^kiji-bento-(?P<release>\w+)-')
    m_bento = p_bento.search(bento_root)
    assert m_bento, bento_root
    return 'kiji-bento-%s' % m_bento.group('release')

  def _setup_environment_vars(self, opts):
    """ Set up useful variables (would be environment vars outside of the script) """
    self.local_env_vars = {}

    self.bento_tgz = self._get_archive_location(opts.bento_tgz, "kiji-bento")
    self.local_bento_dir = self._get_bento_dir_from_archive_name(self.bento_tgz)

    self.local_retail_dir = opts.retail_home
    assert os.path.isdir(self.local_retail_dir)

    self.local_demo_dir = opts.demo_home
    assert os.path.isdir(self.local_demo_dir)

    host = 'localhost'
    self.kiji_uri_retail = "kiji-cassandra://{host}/{host}/retail".format(host=host)
    self.kiji_uri_bestbuy = "kiji-cassandra://{host}/{host}/bestbuy".format(host=host)

    self.express_import_jar = \
        os.path.join(self.local_demo_dir, "express-import", "target", "express-import-0.1.0-SNAPSHOT.jar")
    assert os.path.isfile(self.express_import_jar)

    self.retail_layout_jar = \
        os.path.join(self.local_retail_dir, 'layouts', 'target', 'retail-layouts-0.3.0-SNAPSHOT.jar')
    assert os.path.isfile(self.retail_layout_jar)

    self.retail_models_jar = \
        os.path.join(self.local_retail_dir, 'models', 'target', 'retail-models-0.3.0-SNAPSHOT.jar')
    assert os.path.isfile(self.retail_models_jar)

    self.local_env_vars = {
        'KIJI_CLASSPATH': self._get_local_kiji_classpath(),
    }

    self.bulk_import_lib_dir = "bulk-import-lib"
    self.batch_train_lib_dir = "batch-train-lib"

    print tabulate.tabulate([
        ["Bento Box", self.bento_tgz],
        ["Bento directory", self.local_bento_dir],
        ["WibiRetail directory", self.local_retail_dir],
        ["Retail Demo directory", self.local_demo_dir],
        ["Retail Demo Express import job JAR", self.express_import_jar],
        ["WibiRetail layouts JAR", self.retail_layout_jar],
        ["WibiRetail models (training) JAR", self.retail_models_jar],
        ['Retail URI', self.kiji_uri_retail],
        ['Best Buy URI', self.kiji_uri_bestbuy],
    ])

  def _get_local_kiji_classpath(self):
    """
    Construct the KIJI_CLASSPATH for running local Kiji commands.

    Contains the various Bento JARs, plus bulk importers from the retail demo, etc.

    :return: A string containing the KIJI_CLASSPATH for local Kiji commands, of the form path0:path1:path2...
    """

    components = [
      '{bento}/lib/*',
      '{retail}/layouts/lib/*',
      self.express_import_jar,
      self.retail_layout_jar,
    ]
    self.local_model_repo_dir = "/tmp/retail-model-repo"
    self.local_bestbuy_data_location = "/Users/clint/play/wibiretail/best_buy_small"
    # TODO: Rename!
    self.hadoop_root = "hdfs:///user/clint/bestbuy"

    return ":".join([component.format(bento=self.local_bento_dir, retail=self.local_retail_dir) for component in components])

  def _parse_options(self, args):
    """ Parse the command-line options and configure the script appropriately """
    parser = self._setup_parser()
    opts = parser.parse_args(args)

    self.actions = opts.action
    for action in self.actions:
      assert action in possible_actions, "Action %s is not a known action for the script" % action
    if 'help' in self.actions:
      self._help_actions()

    self._setup_environment_vars(opts)

  # ----------------------------------------------------------------------------------------------
  # Utility methods
  def _run_kiji_command(self, kiji_command, additional_kiji_classpath=""):
    """
    Run a Kiji command on the remote server with the proper environment setup.
    :param kiji_command: The command to run.
    :param additional_kiji_classpathd Additional items to put on KIJI_CLASSPATH (e.g. bento/model_repo/lib for running a model repo command)
    """
    kiji_env = os.path.join(self.local_bento_dir, "bin", "kiji-env.sh")
    cmd_string = "source {kiji_env}; {command}".format(
      kiji_env=kiji_env,
      command=kiji_command
    )
    my_local_env_vars = self.local_env_vars.copy()
    kiji_classpath = my_local_env_vars.get('KIJI_CLASSPATH', "")
    #my_local_env_vars['HADOOP_USER_NAME'] ='root'
    my_local_env_vars['KIJI_CLASSPATH'] = kiji_classpath + ":" + additional_kiji_classpath
    #my_local_env_vars['HADOOP_HOME'] = '/usr/local/opt/hadoop/libexec'
    #my_local_env_vars['HBASE_HOME'] = '/usr/local/opt/hbase/libexec'
    with fabric.api.shell_env(**my_local_env_vars):
      fabric.api.local(cmd_string)

  # ----------------------------------------------------------------------------------------------
  # Methods for installing the Bento Box.
  # TODO: Refactor these methods to share code for opening file, reading contents, writing new file.
  # Can write a common method with a single arg, which is a method that converts old file contents to new file
  # contents (functional programming, hooray!).

  def _unzip_bento_box(self):
    """ Unzip the Bento Box. """
    cmd = "rm -rf {bento_dir}; tar -zxvf {bento_tar}".format(
      bento_dir=self.local_bento_dir,
      bento_tar=self.bento_tgz)
    fabric.api.local(cmd)
    assert os.path.isdir(self.local_bento_dir)

  def _fix_scoring_server_bash_script(self):
    """ Remove erroneous reference in to Jenkins's $JAVA_HOME. """
    scoring_server_bash = os.path.join(self.local_bento_dir, "scoring-server", "bin", "kiji-scoring-server")
    assert os.path.isfile(scoring_server_bash), scoring_server_bash

    ss_for_read = open(scoring_server_bash)
    txt = ss_for_read.read()
    ss_for_read.close()

    txt = txt.replace("/var/lib/jenkins/tools/hudson.model.JDK/Sun_Java_6u39", "${JAVA_HOME}")
    ss_for_write = open(scoring_server_bash, "w")
    ss_for_write.write(txt)
    ss_for_write.close()

  def _fix_scoring_server_config_json(self):
    """ Update the scoring server JSON config file to match desired settings for WibiRetail. """
    scoring_server_json = os.path.join(self.local_bento_dir, "scoring-server", "conf", "configuration.json")
    assert os.path.isfile(scoring_server_json)

    ss_for_write = open(scoring_server_json, "w")
    json = """\
            {
              "port": 7080,
              "repo_uri": "%s",
              "repo_scan_interval": 0,
              "num_acceptors": 2
            }""" % self.kiji_uri_retail
    ss_for_write.write(textwrap.dedent(json))
    ss_for_write.close()

  def _fix_scoring_server_lib(self):
    """ Add JARs needed for Cassandra schema to the scoring server lib. """
    jars_to_copy = [
      "lib/kiji-schema-cassandra-1.6.0-SNAPSHOT.jar",
      "lib/kiji-schema-1.6.0-SNAPSHOT.jar",
      "lib/guava-15.0.jar",
      "lib/cassandra-driver-core-2.0.4.jar",
      "lib/metrics-core-3.0.2.jar",
      #"model-repo/lib/kiji-model-repository-0.11.0-SNAPSHOT.jar",
    ]

    for jar in jars_to_copy:
      orig = os.path.join(self.local_bento_dir, jar)
      assert os.path.isfile(orig), orig
      dest = os.path.join(self.local_bento_dir, "scoring-server", "lib", os.path.basename(jar))
      shutil.copyfile(orig, dest)

    jars_to_remove = [
      'guava-14.0.1.jar',
      #'kiji-model-repository-0.10.0.jar',
    ]

    for jar in jars_to_remove:
      orig = os.path.join(self.local_bento_dir, "scoring-server", "lib", jar)
      assert os.path.isfile(orig), orig
      os.remove(orig)

  def _do_action_install_bento(self):
    """
    Unzip the bento box, then make some tweaks:
    - Modify the bin/kiji script such that it does not exit silently
    - Modify the scoring-server/conf/configuration.json appropriately
    - Modify the scoring server executable to remove reference to Jenkins's $JAVA_HOME
    - Modify express.sh such that it echos all commands and such that it removes any non-15 Guava from the classpath.
    - Copy some JARs to the scoring server's lib directory
    - Tar it back up to copy to the server.

    """

    self._unzip_bento_box()
    self._fix_scoring_server_bash_script()
    self._fix_scoring_server_config_json()
    self._fix_scoring_server_lib()

  # ----------------------------------------------------------------------------------------------
  # Install Kiji instances for Best Buy, Retail.
  def _do_action_install_kiji(self):
    for uri in [self.kiji_uri_bestbuy, self.kiji_uri_retail]:
      self._run_kiji_command("{bento_dir}/bin/kiji install --kiji={kiji}".format(bento_dir=self.local_bento_dir, kiji=uri))

  # ----------------------------------------------------------------------------------------------
  # Set up the model repo remotely.
  def _do_action_install_model_repo(self):
    """
    Set up the model repository.

    Create a directory for models and call `kiji model-repo init`
    """

    model_repo_ls = fabric.api.local("ls %s" % self.local_model_repo_dir)

    # This returns a subclass of string with some attributes.
    if (model_repo_ls.failed):
      print "Model repo directory %s not found, creating it." % self.local_model_repo_dir
      fabric.api.run("mkdir %s" % self.local_model_repo_dir)
    else:
      print "Found model repo dir!"

    # Run the Kiji command to set up the model repo.
    self._run_kiji_command(
      "kiji model-repo init --kiji={kiji_uri} file://{model_repo}".format(
        kiji_uri=self.kiji_uri_retail,
        model_repo=self.local_model_repo_dir
      ),
      additional_kiji_classpath='{bento}/model-repo/lib/*'.format(bento=self.local_bento_dir)
    )

  # ----------------------------------------------------------------------------------------------
  # Start the scoring server.
  def _do_action_start_scoring_server(self):
    """
    Update the system table, start the scoring server, sanity check.

    Make sure to kill any previously-running scoring servers.

    """

    # Run this locally
    system_table_command = """\
            kiji system-table --kiji={kiji} --interactive=false
            --do=put org.kiji.scoring.lib.server.ScoringServerScoreFunction.base_url_key
            http://infra01.ul.wibidata.net:7080""".format(kiji=self.kiji_uri_retail)
    self._run_kiji_command(format_multiline_command(system_table_command))

    def start_scoring_server():
      jps_results = fabric.api.run("/usr/java/jdk1.7.0_51/bin/jps")
      # Kill the scoring server
      for line in jps_results.splitlines():
        toks = line.split()
        if len(toks) == 1: continue
        assert len(toks) == 2, toks
        (pid, job) = toks
        if job != 'ScoringServer': continue
        cmd = "kill -9 " + pid
        fabric.api.local(cmd)
      self._run_kiji_command("{}/scoring-server/bin/kiji-scoring-server".format(self.local_bento_dir))
      fabric.api.local('cat {}/scoring-server/logs/console.out'.format(self.local_bento_dir))

    start_scoring_server()

  # ----------------------------------------------------------------------------------------------
  # Create the tables for wibi retail.
  def _create_wibiretail_tables(self):
    """ Create the WibiRetail tables.  Call Kiji commands on the client. """
    for ddl in ['users-table', 'products-table', 'product-lists-table']:
      cmd = "{bento_dir}/schema-shell/bin/kiji-schema-shell --kiji={kiji} --file={retail}/layouts/src/main/resources/com/wibidata/retail/layouts/ddl/{ddl}.ddl".format(
        bento_dir=self.local_bento_dir,
        kiji=self.kiji_uri_retail,
        retail=self.local_retail_dir,
        ddl=ddl
      )
      self._run_kiji_command(cmd)

  def _create_bestbuy_tables(self):
    """ Create the special Best Buy table. """
    ddl = os.path.join(self.local_demo_dir, 'express-import', 'src', 'main', 'resources', 'com', 'wibidata', 'demo', 'retail', 'bulkimport', 'ddl', 'product-table.ddl')
    assert os.path.isfile(ddl), ddl
    cmd = "{bento_dir}/schema-shell/bin/kiji-schema-shell --kiji={kiji_bb} --file={ddl}".format(
      bento_dir=self.local_bento_dir,
      kiji_bb=self.kiji_uri_bestbuy,
      ddl=ddl
    )
    self._run_kiji_command(cmd)

  def _do_action_create_tables(self):
    self._create_wibiretail_tables()
    self._create_bestbuy_tables()

  # ----------------------------------------------------------------------------------------------
  # Copy Best Buy data to HDFS.

  def _do_action_copy_bestbuy_data_to_hdfs(self):
    """ Copy all of the Best Buy data from the normal file system to HDFS. """

    #with fabric.api.shell_env(HADOOP_CONF_DIR=self.hadoop_conf_dir, HADOOP_USER_NAME='hdfs'):
    # Check that the Best Buy data exists.
    fabric.api.local('ls %s' % self.local_bestbuy_data_location)

    # Make a directory in HDFS.
    fabric.api.local('hadoop fs -mkdir -p %s' % self.hadoop_root)
    fabric.api.local('hadoop fs -test -e %s' % self.hadoop_root)
    # TODO: Remove hard-code for username "clint"
    fabric.api.local('hadoop fs -chown -R clint %s' % self.hadoop_root)

    # TODO: Abort if data has already been copied?

    # Copy the files.
    fabric.api.local('hadoop fs -put -f {input_dir} {hdfs_dir}'.format(
      input_dir=self.local_bestbuy_data_location,
      hdfs_dir=self.hadoop_root
    ))

  # ----------------------------------------------------------------------------------------------
  # Utility stuff for calculating and copying dependencies for jobs.

  def _get_runtime_classpath_for_jar(self, jar):
    target_directory = os.path.dirname(jar)
    assert not target_directory.endswith('/')
    (maven_source_directory, target_directory_short) = os.path.split(target_directory)
    assert target_directory_short == 'target', target_directory_short

    # Classpath text file should be in root maven directory.
    classpath_text_file = os.path.join(maven_source_directory, 'runtime_classpath.txt')
    if not os.path.isfile(classpath_text_file):
      fabric.api.local('cd {maven_dir}; mvn dependency:build-classpath -Dmdep.outputFile={ofile} -DincludeScope=runtime'.format(
        ofile=os.path.basename(classpath_text_file),
        maven_dir=maven_source_directory,
      ))
    assert os.path.isfile(classpath_text_file)
    classpath_one_line = open(classpath_text_file).read()
    return classpath_one_line.split(':')

  def _create_lib_dir_for_jar(self, mainjar, lib_dir, other_jars):
    if os.path.isdir(lib_dir):
      shutil.rmtree(lib_dir)
    os.mkdir(lib_dir)
    for jar in other_jars + [mainjar,]:
      assert os.path.isfile(jar), jar
      shutil.copyfile(jar, os.path.join(lib_dir, os.path.basename(jar)))

  # ----------------------------------------------------------------------------------------------
  # Prepare for running the bulk importers (copy JARs to the server)


  def _do_action_prepare_for_bulk_import(self):
    """ Prepare for running the bulk importer by copying JARs to the server. """
    express_import_jars = self._get_runtime_classpath_for_jar(self.express_import_jar)

    self._create_lib_dir_for_jar(
      mainjar = self.express_import_jar,
      lib_dir = self.bulk_import_lib_dir,
      other_jars=express_import_jars + [self.retail_layout_jar,],
    )

  # ----------------------------------------------------------------------------------------------
  # Bulk import the data using KijiExpress!!!!!
  def _bulk_load_demo(self):
    """ Run the DemoProductImporter job.  """

    print "Bulk loading the retail demo data..."

    assert os.path.isdir(self.bulk_import_lib_dir)

    cmd = format_multiline_command("""\
            {bento_dir}/express/bin/express.py
                --log-level debug
                job
                --jars='{bulk_import_lib_dir}/*'
                --mode=hdfs
                --class=com.wibidata.demo.retail.bulkimport.DemoProductImporter
                --products-table {bb_kiji}/product
                --review-input {hadoop_root}/best_buy_small/formatted_reviews.json
                --aux-output {hadoop_root}/aux-output
                --scalding-exception-trap {hadoop_root}/bb_importer_trap/
                --input {hadoop_root}/best_buy_small/products""".format(
      bento_dir=self.local_bento_dir,
      kiji=self.kiji_uri_retail,
      retail_jar=self.express_import_jar,
      hadoop_root=self.hadoop_root,
      bb_kiji=self.kiji_uri_bestbuy,
      bulk_import_lib_dir=self.bulk_import_lib_dir,
    ))
    self._run_kiji_command(cmd)


  def _bulk_load_retail(self):
    """ Run the RetailProductImporter job.  """
    print "Bulk loading the WibiRetail data..."

    cmd = format_multiline_command("""\
            express.py job
                --log-level=debug
                --jars '{bulk_import_lib_dir}/*'
                --mode=hdfs
                --class=com.wibidata.demo.retail.bulkimport.RetailProductImporter
                --instance-uri {kiji}
                --product-input {hadoop_root}/best_buy_small/products
                --review-input {hadoop_root}/best_buy_small/formatted_reviews.json
                --scalding-exception-trap {hadoop_root}/wr_importer_trap/""".format(
      kiji=self.kiji_uri_retail,
      retail_jar=self.express_import_jar,
      hadoop_root=self.hadoop_root,
      bb_kiji=self.kiji_uri_bestbuy,
      bulk_import_lib_dir=self.bulk_import_lib_dir,
    ))
    self._run_kiji_command(cmd)

  def _do_action_bulk_import(self):
    """ Actually bulk import the Best Buy data!!!! """
    self._bulk_load_demo()
    self._bulk_load_retail()

  # ----------------------------------------------------------------------------------------------
  # Prepare for batch training

  def _do_action_prepare_for_batch_train(self):
    """ Prepare for running the bulk importer by copying JARs to the server. """
    model_dependency_jars = self._get_runtime_classpath_for_jar(self.retail_models_jar)

    self._create_lib_dir_for_jar(
        mainjar = self.retail_models_jar,
        lib_dir=self.batch_train_lib_dir,
        other_jars=model_dependency_jars,
    )

  # ----------------------------------------------------------------------------------------------
  # Run all of the training algorithms in batch.
  def _batch_train_product_similarity_by_text(self):
    """ Run the batch trainer for product similarity. """
    print 'Running the product similarity job...'
    cmd = format_multiline_command("""\
            express.py job
                --log-level=debug
                --jars '{lib_dir}/*'
                --mode=hdfs
                --class=com.wibidata.retail.models.batch.ProductSimilarityByTextJob
                --do-direct-writes
                --instance-uri {kiji}""".format(
      kiji=self.kiji_uri_retail,
      lib_dir=self.batch_train_lib_dir,
    ))
    self._run_kiji_command(cmd)

  def _batch_train_customers_also_purchased(self):
    """ Batch train """
    print 'Running the customers-also-purchased job...'
    cmd = format_multiline_command("""\
            express.py job
                --log-level=debug
                --jars '{lib_dir}/*'
                --mode=hdfs
                --class=com.wibidata.retail.models.batch.CustomersAlsoPurchasedJob
                --do-direct-writes
                --instance-uri {kiji}""".format(
      kiji=self.kiji_uri_retail,
      lib_dir=self.batch_train_lib_dir,
    ))
    self._run_kiji_command(cmd)

  def _batch_train_customers_also_viewed(self):
    """ Batch train """
    print 'Running the customers-also-viewed job...'
    cmd = format_multiline_command("""\
            express.py job
                --log-level=debug
                --jars '{lib_dir}/*'
                --mode=hdfs
                --class=com.wibidata.retail.models.batch.CustomersAlsoViewedJob
                --do-direct-writes
                --instance-uri {kiji}""".format(
      kiji=self.kiji_uri_retail,
      lib_dir=self.batch_train_lib_dir,
    ))
    self._run_kiji_command(cmd)

  def _do_action_batch_train(self):
    #self._batch_train_product_similarity_by_text() # WAY TOO BIG FOR THE LAPTOP!
    self._batch_train_customers_also_purchased()
    self._batch_train_customers_also_viewed()

  # ----------------------------------------------------------------------------------------------
  # Get Jetty and SOLR started.
  def _do_action_start_jetty(self):
    with fabric.api.lcd(os.path.join(self.local_demo_dir, 'solr-search')):
      fabric.api.local('mvn -Djetty.port=8983 jetty:run')
    # Poll until Jetty actually starts
    for i in range(10):
      time.sleep(20)
      conn = urllib2.request_host("http://localhost:8983/solr/bestbuy/select")
      if conn.get


  # ----------------------------------------------------------------------------------------------
  # Main method.
  def _run_actions(self):
    """ Run whatever actions the user has specified """

    if "install-bento" in self.actions:
      self._do_action_install_bento()

    if 'install-kiji' in self.actions:
      self._do_action_install_kiji()

    if "install-model-repo" in self.actions:
      self._do_action_install_model_repo()

    if "start-scoring-server" in self.actions:
      self._do_action_start_scoring_server()

    if "create-tables" in self.actions:
      self._do_action_create_tables()

    if "copy-bb-data-to-hdfs" in self.actions:
      self._do_action_copy_bestbuy_data_to_hdfs()

    if 'prepare-bulk-import' in self.actions:
      self._do_action_prepare_for_bulk_import()

    if 'bulk-import' in self.actions:
      self._do_action_bulk_import()

    if 'prepare-batch-train' in self.actions:
      self._do_action_prepare_for_batch_train()

    if 'batch-train' in self.actions:
      self._do_action_batch_train()

    if 'start-jetty' in self.actions:
      self._do_action_start_jetty()

  def go(self, args):
    try:
      self._parse_options(args)
      self._run_actions()
    finally:
      fabric.network.disconnect_all()


if __name__ == "__main__":
  InfraManager().go(sys.argv[1:])
