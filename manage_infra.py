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
import pprint
import subprocess
import sys
import re
import shutil
import textwrap
from argparse import RawTextHelpFormatter

# Command-line "actions" the user can use and the corresponding help messages.
actions_help = {}

# Maintains order of possible actions for help.
possible_actions = []

def add_action(action_name, help_text):
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

add_action("install-model-repo", "Create a directory on the server for the model repo and initialize the mode repo.")

add_action("start-scoring-server", "Update the system table and start the scoring server, then verify that it is working.")

add_action("create-tables", "Create the WibiRetail tables.")

add_action("copy-bb-data-to-hdfs", "Copy the Best Buy data onto HDFS in the cluster.")

add_action('prepare-bulk-import', 'Create lib dir for bulk import job, copy all JARs to cluster.')

add_action('bulk-import', 'Bulk import the Best Buy data to Kiji')

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
        for (key, value) in actions_help.items():
            actions_str += "command: %s\n%s\n\n" % (key, value)
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
            '--retail-tgz',
            help='WibiRetail archive file name (default is wibi-retail-*-tar.gz in pwd)',
            default=None)

        parser.add_argument(
            '--demo-home',
            help='Home directory for the retail demo (assumed to be source, after running "mvn clean package"' +
                 ' (Default is pwd/retail-demo)',
            default='retail-demo')

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

    def _get_retail_dir_from_archive_name(self, retail_tgz):
        retail_root = os.path.basename(retail_tgz)
        p_retail = re.compile(r'wibi-retail-(?P<version>\d+\.\d+\.\d+(-SNAPSHOT)?)-release.tar.gz')
        m_retail = p_retail.match(retail_root)
        assert m_retail
        return 'wibi-retail-%s' % m_retail.group('version')

    def _setup_environment_vars(self, opts):
        """ Set up useful variables (would be environment vars outside of the script) """

        self.bento_tgz = self._get_archive_location(opts.bento_tgz, "kiji-bento")
        self.retail_tgz = self._get_archive_location(opts.retail_tgz, "wibi-retail")

        self.local_bento_dir = self._get_bento_dir_from_archive_name(self.bento_tgz)
        self.local_retail_dir = self._get_retail_dir_from_archive_name(self.retail_tgz)

        self.local_demo_dir = opts.demo_home
        assert os.path.isdir(self.local_demo_dir)
        # TODO: Sanity check that JARs are present here for Avro records.

        if not os.path.isdir(self.local_retail_dir):
            fabric.api.local("tar -zxvf %s" % self.retail_tgz)
        assert os.path.isdir(self.local_retail_dir)

        print "Bento Box:            " + self.bento_tgz
        print "Local bento dir:      " + self.local_bento_dir
        print "Wibi Retail:          " + self.retail_tgz
        print "Local retail dir:     " + self.local_retail_dir

        # Name of archive created from edited Bento Box (to send to infra cluster).
        self.cluster_tgz = "cluster.tar.gz"
        self.kiji_uri_retail = "kiji-cassandra://infra02.ul.wibidata.net/infra02.ul.wibidata.net/retail"
        self.kiji_uri_bestbuy = "kiji-cassandra://infra02.ul.wibidata.net/infra02.ul.wibidata.net/bestbuy"

        self.express_import_jar = \
            os.path.join(self.local_demo_dir, "express-import", "target", "express-import-0.1.0-SNAPSHOT.jar")
        assert os.path.isfile(self.express_import_jar)

        self.retail_layout_jar = \
            os.path.join(self.local_retail_dir, 'layouts', 'lib', 'retail-layouts-0.2.0.jar')
        assert os.path.isfile(self.retail_layout_jar)

        self.local_env_vars = {
            'KIJI_CLASSPATH': self._get_local_kiji_classpath(),
        }

        self.bulk_import_lib_dir = "bulk-import-lib"

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

        return ":".join([component.format(bento=self.local_bento_dir, retail=self.local_retail_dir) for component in components])

    def _setup_remote_environment_vars(self, opts):
        """
        Set any settings / variables for the cluster.

        :param opts: Command line options for the entire script.
        """
        self.remote_bento_dir = self.local_bento_dir
        self.remote_host = "infra01.ul.wibidata.net"
        self.remote_model_repo_dir = "/tmp/retail-model-repo"
        self.remote_env_vars = {
            'JAVA_HOME' : '/usr/java/jdk1.7.0_51',
            'HADOOP_HOME' : '/usr/lib/hadoop',
            'HBASE_HOME' : '/usr/lib/hbase',
            #'KIJI_CLASSPATH': '{bento}/lib/*:{bento}/model-repo/lib/*:{bento}/scoring-server/lib/*'.format(
                #bento=self.remote_bento_dir
            #),
        }

        self.remote_bestbuy_data_location = \
            "/net/infra01/var/ssd/share/datasets/retail_demo_data/best_buy_small/"
        self.hadoop_root = "hdfs:///user/clint/bestbuy"

        # Just stick in the home directory.
        self.remote_express_import_jar = os.path.basename(self.express_import_jar)
        self.remote_retail_layout_jar = os.path.basename(self.retail_layout_jar)

    def _parse_options(self, args):
        """ Parse the command-line options and configure the script appropriately """
        parser = self._setup_parser()
        opts = parser.parse_args(args)

        self.actions = opts.action
        for action in self.actions:
            assert action in possible_actions, "Action %s is not a known action for the script" % action
        if 'help' in self.actions: self._help_actions()

        self._setup_environment_vars(opts)
        self._setup_remote_environment_vars(opts)

    # ----------------------------------------------------------------------------------------------
    # Utility methods for doing things on the server.
    def _run_remote_kiji_command(self, kiji_command, pty=True, additional_kiji_classpath=""):
        """
        Run a Kiji command on the remote server with the proper environment setup.
        :param kiji_command: The command to run.
        :param pty: Pty parameter to pass to fabric (false for scoring server)
        :param additional_kiji_classpathd Additional items to put on KIJI_CLASSPATH (e.g. bento/model_repo/lib for running a model repo command)
        """
        kiji_env = os.path.join(self.remote_bento_dir, "bin", "kiji-env.sh")
        cmd_string = "source %s; %s" % (kiji_env, kiji_command)

        # Update the environment variables possibly with additional stuff on the classpath.
        my_remote_env_vars = self.remote_env_vars.copy()
        kiji_classpath = my_remote_env_vars.get('KIJI_CLASSPATH', "")
        my_remote_env_vars['KIJI_CLASSPATH'] = kiji_classpath + ":" + additional_kiji_classpath

        with fabric.api.shell_env(**my_remote_env_vars):
            fabric.api.run(cmd_string, pty=pty)

    def _run_kiji_command(self, kiji_command):
        kiji_env = os.path.join(self.local_bento_dir, "bin", "kiji-env.sh")
        cmd_string = "source %s; %s" % (kiji_env, kiji_command)
        with fabric.api.shell_env(**self.local_env_vars):
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

    def _fix_kiji_bash_script(self):
        """ Remove lines about upgrade_informer_script. """
        # Read until the line that contains "upgrade_informer_script", then skip four lines
        kiji_bash = os.path.join(self.local_bento_dir, "bin", "kiji")
        assert os.path.isfile(kiji_bash)

        # Read contents of script.
        kiji_bash_file_for_read = open(kiji_bash)
        txt = kiji_bash_file_for_read.read()
        kiji_bash_file_for_read.close()

        # Write out everything except for bad lines.
        kiji_bash_file_for_write = open(kiji_bash, "w")
        lines = txt.splitlines(True)
        line_num = 0
        while line_num < len(lines):
          myline = lines[line_num]
          if myline.startswith("upgrade_informer_script"):
            line_num += 4
            continue
          kiji_bash_file_for_write.write(myline)
          line_num += 1
        kiji_bash_file_for_write.close()

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
            "lib/cassandra-driver-core-2.0.3.jar",
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

    def _fix_express_sh(self):
        """
        Update express.sh:
        - Add a "set -x" command to the beginning of the file to turn on automatic echoing of everything
        - Add a sed command that will remove any Guava from the classpath that is not Guava 15.

        """

        # Read the original contents of the file.
        express_sh_file = os.path.join(self.local_bento_dir, "express", "bin", "express.sh")
        assert os.path.isfile(express_sh_file)
        express_sh_for_read = open(express_sh_file)
        express_sh_text = express_sh_for_read.read()
        express_sh_for_read.close()

        # Insert the set -x right after the first line.
        # Insert the sed stuff right after the assignment to "user_argv" (I found this position just by looking at
        # the source file - there is nothing special about this assignment.  It it just the last thing that happens
        # before running the Java commands.)
        express_sh_for_write = open(express_sh_file, 'w')
        b_fixed_set = False
        b_fixed_guava = False
        for line in express_sh_text.splitlines(True):
            express_sh_for_write.write(line)
            if line.startswith('#!/usr/bin/env bash'):
                assert not b_fixed_set
                express_sh_for_write.write('set -x\n')
                b_fixed_set = True
            if line.startswith('  user_argv='):
                assert not b_fixed_guava
                express_sh_for_write.write('  # Use sed to remove any guava except for guava 15 from the tmp jars.\n')
                express_sh_for_write.write("  tmpjars=$(echo ${tmpjars}  | sed 's|file:///\([^/^,]*/\)*guava-1[012346789]\(\.[0-9]\)*\.jar,\{0,1\}||g' )\n\n")
                b_fixed_guava = True
        assert b_fixed_guava and b_fixed_set
        express_sh_for_write.close()

    def _archive_bento_for_cluster(self):
        """ Zip up the modified bento box and ship off to a cluster! """

        cmd = "tar -czvf {cluster_tar} {bento_home}".format(
            cluster_tar=self.cluster_tgz,
            bento_home=self.local_bento_dir
        )
        fabric.api.local(cmd)

        assert os.path.isfile(self.cluster_tgz), "Could not find %s." % self.cluster_tgz


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
        #self._fix_kiji_bash_script()
        self._fix_scoring_server_bash_script()
        self._fix_scoring_server_config_json()
        self._fix_scoring_server_lib()
        self._fix_express_sh()
        self._archive_bento_for_cluster()



    # ----------------------------------------------------------------------------------------------
    # Copy the Bento tar file (post-editing) to the cluster.
    def _do_action_copy_bento(self):
        """ scp the bento box over to the cluster and untar it. """

        def copy_bento():
            """ Wrap all of this stuff into a function to call with fabric's "execute" """
            assert os.path.isfile(self.cluster_tgz)

            # Actually run the scp command.
            fabric.api.put(self.cluster_tgz)

            # Remove the path from the name and get just the file name.
            cluster_tar_name = os.path.basename(self.cluster_tgz)

            # Delete what was there before.
            fabric.api.run("rm -rf %s" % self.remote_bento_dir)

            # Untar it!
            fabric.api.run("tar -zxvf %s" % cluster_tar_name)

        fabric.api.execute(copy_bento, host=self.remote_host)

    # ----------------------------------------------------------------------------------------------
    # Install Kiji instances for Best Buy, Retail.
    def _do_action_install_kiji(self):
        for uri in [self.kiji_uri_bestbuy, self.kiji_uri_retail]:
            self._run_kiji_command("kiji install --kiji={kiji}".format(kiji=uri))

    # ----------------------------------------------------------------------------------------------
    # Set up the model repo remotely.
    def _do_action_install_model_repo(self):
        """
        Set up the model repository.

        Create a directory for models and call `kiji model-repo init`
        """

        def install_model_repo():
            model_repo_ls = fabric.api.run("ls %s" % self.remote_model_repo_dir, quiet=True)

            # This returns a subclass of string with some attributes.
            if (model_repo_ls.failed):
                print "Model repo directory %s not found, creating it." % self.remote_model_repo_dir
                fabric.api.run("mkdir %s" % self.remote_model_repo_dir)
            else:
                print "Found model repo dir!"

            # Run the Kiji command to set up the model repo.
            self._run_remote_kiji_command(
                "kiji model-repo init --kiji={kiji_uri} file://{model_repo}".format(
                    kiji_uri=self.kiji_uri_retail,
                    model_repo=self.remote_model_repo_dir
                ),
                additional_kiji_classpath='{bento}/model-repo/lib/*'.format(bento=self.remote_bento_dir)
            )

        fabric.api.execute(install_model_repo, host=self.remote_host)

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
                fabric.api.run(cmd)
            self._run_remote_kiji_command(
                "{}/scoring-server/bin/kiji-scoring-server".format(self.remote_bento_dir),
                pty=False)
            fabric.api.run('cat {}/scoring-server/logs/console.out'.format(self.remote_bento_dir))

        fabric.api.execute(start_scoring_server, host=self.remote_host)

    # ----------------------------------------------------------------------------------------------
    # Create the tables for wibi retail.
    def _create_wibiretail_tables(self):
        """ Create the WibiRetail tables.  Call Kiji commands on the client. """
        for ddl in ['users-table', 'products-table', 'product-lists-table']:
            cmd = "kiji-schema-shell --kiji={kiji} --file={retail}/layouts/ddl/{ddl}.ddl".format(
                kiji=self.kiji_uri_retail,
                retail=self.local_retail_dir,
                ddl=ddl
            )
            self._run_kiji_command(cmd)

    def _create_bestbuy_tables(self):
        """ Create the special Best Buy table. """
        ddl = os.path.join(self.local_demo_dir, 'express-import', 'src', 'main', 'resources', 'com', 'wibidata', 'demo', 'retail', 'bulkimport', 'ddl', 'product-table.ddl')
        assert os.path.isfile(ddl), ddl
        cmd = "kiji-schema-shell --kiji={kiji_bb} --file={ddl}".format(
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

        def copy_bestbuy_data():
            # Check that the Best Buy data exists.
            fabric.api.run('ls %s' % self.remote_bestbuy_data_location)

            # Make a directory in HDFS.
            fabric.api.run('hadoop fs -mkdir -p %s' % self.hadoop_root)
            fabric.api.run('hadoop fs -test -e %s' % self.hadoop_root)

            # TODO: Abort if data has already been copied?

            # Copy the files.
            fabric.api.run('hadoop fs -put -f {input_dir} {hdfs_dir}'.format(
                input_dir=self.remote_bestbuy_data_location,
                hdfs_dir=self.hadoop_root
            ))

        fabric.api.execute(copy_bestbuy_data, host=self.remote_host)

    # ----------------------------------------------------------------------------------------------
    # Prepare for running the bulk importers (copy JARs to the server)
    def _get_bulk_import_classpath(self):
        """
        Generate the bulk import classpath by running mvn dependency:build-classpath.

        :return: A list of JARs.
        """
        classpath_text_file = os.path.join(self.local_demo_dir, 'express-import', 'runtime_classpath.txt')
        if not os.path.isfile(classpath_text_file):
            fabric.api.local('cd {express_import_dir}; mvn dependency:build-classpath -Dmdep.outputFile={ofile} -DincludeScope=runtime'.format(
                ofile=os.path.basename(classpath_text_file),
                express_import_dir=os.path.dirname(classpath_text_file)
            ))
        assert os.path.isfile(classpath_text_file)
        classpath_one_line = open(classpath_text_file).read()
        return classpath_one_line.split(':')

    def _create_bulk_import_lib_dir(self, classpath):
        assert os.path.isdir(self.bulk_import_lib_dir)
        for jar in classpath:
            assert os.path.isfile(jar), jar
            shutil.copyfile(jar, os.path.join(self.bulk_import_lib_dir, os.path.basename(jar)))

    def _do_action_prepare_for_bulk_import(self):
        """ Prepare for running the bulk importer by copying JARs to the server. """

        # Get the classpath for the bulk import job by running mvn dependency:build-classpath -DincludeScope=runtime.
        # TODO: Need to remember the order for these JARs?
        bulk_import_classpath = self._get_bulk_import_classpath()
        fabric.api.local('mkdir -p %s' % self.bulk_import_lib_dir)
        self._create_bulk_import_lib_dir(bulk_import_classpath)

        def prepare_bulk_load():
            fabric.api.put(self.express_import_jar)
            fabric.api.put(self.retail_layout_jar)
            fabric.api.put(self.bulk_import_lib_dir)

        fabric.api.execute(prepare_bulk_load, host=self.remote_host)

    # ----------------------------------------------------------------------------------------------
    # Bulk import the data using KijiExpress!!!!!
    def _bulk_load_demo(self):
        """ Run the DemoProductImporter job.  """

        print "Bulk loading the retail demo data..."

        cmd = format_multiline_command("""\
            express job
                --libjars {layout_jar}:{bulk_import_lib_dir}/*
                {retail_jar}
                com.wibidata.demo.retail.bulkimport.DemoProductImporter
                --products-table {bb_kiji}/product
                --review-input {hadoop_root}/best_buy_small/formatted_reviews.json
                --aux-output {hadoop_root}/aux-output
                --scalding-exception-trap {hadoop_root}/bb_importer_trap/
                --input {hadoop_root}/best_buy_small/products
                --hdfs""".format(
            kiji=self.kiji_uri_retail,
            retail_jar=self.remote_express_import_jar,
            layout_jar=self.remote_retail_layout_jar,
            hadoop_root=self.hadoop_root,
            bb_kiji=self.kiji_uri_bestbuy,
            bulk_import_lib_dir=self.bulk_import_lib_dir,
        ))
        self._run_remote_kiji_command(cmd)


    def _bulk_load_retail(self):
        """ Run the RetailProductImporter job.  """
        print "Bulk loading the WibiRetail data..."

        cmd = format_multiline_command("""\
            express job
                --libjars {layout_jar}:{bulk_import_lib_dir}/*
                {retail_jar}
                com.wibidata.demo.retail.bulkimport.RetailProductImporter
                --instance-uri {kiji}
                --product-input {hadoop_root}/best_buy_small/products
                --review-input {hadoop_root}/best_buy_small/formatted_reviews.json
                --scalding-exception-trap {hadoop_root}/wr_importer_trap/
                --hdfs""".format(
            kiji=self.kiji_uri_retail,
            retail_jar=self.remote_express_import_jar,
            layout_jar=self.remote_retail_layout_jar,
            hadoop_root=self.hadoop_root,
            bb_kiji=self.kiji_uri_bestbuy,
            bulk_import_lib_dir=self.bulk_import_lib_dir,
        ))
        self._run_remote_kiji_command(cmd)

    def _do_action_bulk_import(self):
        """ Actually bulk import the Best Buy data!!!! """

        def bulk_load():
            self._bulk_load_demo()
            self._bulk_load_retail()

        fabric.api.execute(bulk_load, host=self.remote_host)

    def _do_action_run_simple(self):
        self.colors_file = "colors.txt"

        def copy_temp_data():
            fabric.api.put(self.colors_file)

            # Check that the colors data was copied.
            fabric.api.run('ls %s' % self.colors_file)

            # Make a directory in HDFS.
            fabric.api.run('hadoop fs -mkdir -p %s' % self.hadoop_root)
            fabric.api.run('hadoop fs -test -e %s' % self.hadoop_root)

            # TODO: Abort if data has already been copied?

            # Copy the files.
            fabric.api.run('hadoop fs -put {colors} {hdfs_dir}'.format(
                colors=self.colors_file,
                hdfs_dir=self.hadoop_root
            ))

        #fabric.api.execute(copy_temp_data, host=self.remote_host)

        def run_express():
            print "Running trivial Express job..."

            cmd = format_multiline_command("""\
                express job
                    --libjars {layout_jar}:{bulk_import_lib_dir}/*
                    {retail_jar}
                    com.wibidata.demo.retail.bulkimport.SimpleImporter
                    --input-file {hadoop_root}/{colors}
                    --output-table {colors_kiji}/simple
                    --hdfs""".format(
                colors_kiji="kiji-cassandra://infra02.ul.wibidata.net/infra02.ul.wibidata.net/simple",
                colors=self.colors_file,
                retail_jar=self.remote_express_import_jar,
                layout_jar=self.remote_retail_layout_jar,
                hadoop_root=self.hadoop_root,
                bulk_import_lib_dir=self.bulk_import_lib_dir,
            ))
            self._run_remote_kiji_command(cmd)

        fabric.api.execute(run_express, host=self.remote_host)


    # ----------------------------------------------------------------------------------------------
    # Main method.
    def _run_actions(self):
        """ Run whatever actions the user has specified """

        if "install-bento" in self.actions:
            self._do_action_install_bento()

        if "copy-bento" in self.actions:
            self._do_action_copy_bento()

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

    def go(self, args):
        try:
            self._parse_options(args)
            self._run_actions()
        finally:
            fabric.network.disconnect_all()


if __name__ == "__main__":
    InfraManager().go(sys.argv[1:])
