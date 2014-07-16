""" Python script to manage running jobs, checking the results, etc. """

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


def run(cmd):
    print(cmd)
    try:
        res = subprocess.check_output(cmd, shell=True).decode('utf-8')
    except subprocess.CalledProcessError as cpe:
        print("Error run command " + cmd)
        print("Output = " + cpe.output.decode('utf-8'))
        raise cpe
    return res


class TimeInterval:
    def __init__(self, start, end):
        """ TODO: Check times formatting properly here """
        self.start = start
        self.end = end

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
            description="Setup WibiRetail on C* on infra cluster:\n\t" + "\n\t".join(possible_actions))

        parser.add_argument(
            "action",
            nargs='*',
            help="Action to take")

        parser.add_argument(
            '--bento-home',
            help='Location of bento box',
            default='kiji-bento-fugu')

        parser.add_argument(
            '--bento-tgz',
            help='Bento TAR file name (default is tar.gz in pwd)',
            default=None)

        parser.add_argument(
            "--show-classpath",
            action="store_true",
            default=False,
            help="Echo $KIJI_CLASSPATH and exit")

        return parser

    def _setup_environment_vars(self, opts):
        """ Set up useful variables (would be environment vars outside of the script) """
        self.local_bento_dir = opts.bento_home
        self.bento_tgz = opts.bento_tgz
        if None == self.bento_tgz:
            self.bento_tgz = self._find_bento_tgz()
        self.cluster_tgz = "cluster.tar.gz"
        self.kiji_uri = "kiji-cassandra://infra02.ul.wibidata.net/infra02.ul.wibidata.net/retail"

        # TODO: Kiji classpath setup
        #os.environ['KIJI_CLASSPATH'] = classpath

        self.remote_bento_dir = os.path.basename(
            self.local_bento_dir if not self.local_bento_dir.endswith("/") else self.local_bento_dir[:-1])

        assert self.remote_bento_dir != ""
        self.remote_host = "infra01.ul.wibidata.net"
        self.remote_model_repo_dir = "/tmp/retail-model-repo"
        self.remote_env_vars = {
            'JAVA_HOME' : '/usr/java/jdk1.7.0_51',
            'HADOOP_HOME' : '/usr/lib/hadoop',
            'HBASE_HOME' : '/usr/lib/hbase',
            'KIJI_CLASSPATH': '{bento}/lib/*:{bento}/model-repo/lib/*:{bento}/scoring-server/lib/*'.format(
                bento=self.remote_bento_dir
            ),
        }
        self.local_env_vars = {
            'KIJI_CLASSPATH': '{bento}/lib/*:{bento}/model-repo/lib/*:{bento}/scoring-server/lib/*'.format(
                bento=self.local_bento_dir
            ),
        }

    def _parse_options(self, args):
        """ Parse the command-line options and configure the script appropriately """
        parser = self._setup_parser()
        opts = parser.parse_args(args)

        self.actions = opts.action
        for action in self.actions:
            assert action in possible_actions, "Action %s is not a known action for the script" % action
        if 'help' in self.actions: self._help_actions()

        self._setup_environment_vars(opts)

    def _find_bento_tgz(self):
        """ Locate the bento box! """
        assert False, "Implement this!"

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
        print(run(cmd))
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
            }""" % self.kiji_uri
        ss_for_write.write(textwrap.dedent(json))
        ss_for_write.close()

    def _fix_scoring_server_lib(self):
        """ Add JARs needed for Cassandra schema to the scoring server lib. """
        jars_to_copy = [
            "kiji-schema-cassandra-1.5.1-SNAPSHOT.jar",
            "guava-15.0.jar",
            "cassandra-driver-core-2.0.3.jar",
            "metrics-core-3.0.2.jar"
        ]
        jars_to_remove = [
            'guava-14.0.1.jar',
        ]

        for jar in jars_to_copy:
            orig = os.path.join(self.local_bento_dir, "lib", jar)
            assert os.path.isfile(orig)
            dest = os.path.join(self.local_bento_dir, "scoring-server", "lib", jar)
            shutil.copyfile(orig, dest)

        for jar in jars_to_remove:
            orig = os.path.join(self.local_bento_dir, "scoring-server", "lib", jar)
            assert os.path.isfile(orig), orig
            os.remove(orig)

    def _archive_bento_for_cluster(self):
        """ Zip up the modified bento box and ship off to a cluster! """

        cmd = "tar -czvf {cluster_tar} {bento_home}".format(
            cluster_tar=self.cluster_tgz,
            bento_home=self.local_bento_dir
        )
        print(run(cmd))

        assert os.path.isfile(self.cluster_tgz), "Could not find %s." % self.cluster_tgz

    def _do_action_install_bento(self):
        """
        Unzip the bento box, then make some tweaks:
        - Modify the bin/kiji script such that it does not exit silently
        - Modify the scoring-server/conf/configuration.json appropriately
        - Modify the scoring server executable to remove reference to Jenkins's $JAVA_HOME
        - Copy some JARs to the scoring server's lib directory
        - Tar it back up to copy to the server.

        """

        self._unzip_bento_box()
        self._fix_kiji_bash_script()
        self._fix_scoring_server_bash_script()
        self._fix_scoring_server_config_json()
        self._fix_scoring_server_lib()
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
                run("mkdir %s" % self.remote_model_repo_dir)
            else:
                print "Found model repo dir!"

            # Run the Kiji command to set up the model repo.
            self._run_remote_kiji_command(
                "kiji model-repo init --kiji={kiji_uri} file://{model_repo}".format(
                    kiji_uri=self.kiji_uri,
                    model_repo=self.remote_model_repo_dir
                ))

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
            http://infra01.ul.wibidata.net:7080""".format(kiji=self.kiji_uri)
        self._run_kiji_command(textwrap.dedent(system_table_command).replace('\n', ' '))

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
    # Utility methods for doing things on the server.
    def _run_remote_kiji_command(self, kiji_command, pty=True):
        """
        Run a Kiji command on the remote server with the proper environment setup.
        :param kiji_command: The command to run.
        :param pty: Pty parameter to pass to fabric (false for scoring server)
        """
        kiji_env = os.path.join(self.remote_bento_dir, "bin", "kiji-env.sh")
        cmd_string = "source %s; %s" % (kiji_env, kiji_command)
        with fabric.api.shell_env(**self.remote_env_vars):
            fabric.api.run(cmd_string, pty=pty)

    def _run_kiji_command(self, kiji_command):
        kiji_env = os.path.join(self.local_bento_dir, "bin", "kiji-env.sh")
        cmd_string = "source %s; %s" % (kiji_env, kiji_command)
        with fabric.api.shell_env(**self.local_env_vars):
            fabric.api.local(cmd_string)

    # ----------------------------------------------------------------------------------------------
    # Main method.
    def _run_actions(self):
        """ Run whatever actions the user has specified """

        if "install-bento" in self.actions:
            self._do_action_install_bento()

        if "copy-bento" in self.actions:
            self._do_action_copy_bento()

        if "install-model-repo" in self.actions:
            self._do_action_install_model_repo()

        if "start-scoring-server" in self.actions:
            self._do_action_start_scoring_server()

    def go(self, args):
        try:
            self._parse_options(args)
            self._run_actions()
        finally:
            fabric.network.disconnect_all()


if __name__ == "__main__":
    InfraManager().go(sys.argv[1:])
