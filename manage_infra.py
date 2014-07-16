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
    actions_help[action_name] = help_text

add_action("help", "Display help about different actions.")
add_action("install-bento",
"""
Will set up a bento box for you.  Assumes that you are in a directory with a tar.gz file for the latest bento build.
This command will rm -rf your current bento box (which is also assumed to be in your current directory).  Also does
stuff like editing out lines that break bin/kiji and updating the scoring server conf file appropriately.
""")
add_action("copy-bento",
"""
Will tar up the current bento box (after whatever edits were made in the previous step, copy it to the infra cluster,
and unpack it.
""")
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
        self.bento_home = opts.bento_home
        self.bento_tgz = opts.bento_tgz
        if None == self.bento_tgz:
            self.bento_tgz = self._find_bento_tgz()
        self.cluster_tgz = "cluster.tar.gz"
        self.kiji_uri = "kiji-cassandra://infra02.ul.wibidata.net/infra02.ul.wibidata.net/retail"

        # TODO: Kiji classpath setup
        #os.environ['KIJI_CLASSPATH'] = classpath

        self.remote_host = "infra01.ul.wibidata.net"

        if opts.show_classpath:
            print("export KIJI_CLASSPATH=%s" % classpath)
            sys.exit(0)

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
            bento_dir=self.bento_home,
            bento_tar=self.bento_tgz)
        print(run(cmd))
        assert os.path.isdir(self.bento_home)

    def _fix_kiji_bash_script(self):
        """ Remove lines about upgrade_informer_script. """
        # Read until the line that contains "upgrade_informer_script", then skip four lines
        kiji_bash = os.path.join(self.bento_home, "bin", "kiji")
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
        scoring_server_bash = os.path.join(self.bento_home, "scoring-server", "bin", "kiji-scoring-server")
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
        scoring_server_json = os.path.join(self.bento_home, "scoring-server", "conf", "configuration.json")
        assert os.path.isfile(scoring_server_json)

        ss_for_write = open(scoring_server_json, "w")
        ss_for_write.write("""
            {
              "port": 7080,
              "repo_uri": "%s",
              "repo_scan_interval": 0,
              "num_acceptors": 2
            }""" % self.kiji_uri)
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
            orig = os.path.join(self.bento_home, "lib", jar)
            assert os.path.isfile(orig)
            dest = os.path.join(self.bento_home, "scoring-server", "lib", jar)
            shutil.copyfile(orig, dest)

        for jar in jars_to_remove:
            orig = os.path.join(self.bento_home, "scoring-server", "lib", jar)
            assert os.path.isfile(orig), orig
            os.remove(orig)

    def _archive_bento_for_cluster(self):
        """ Zip up the modified bento box and ship off to a cluster! """

        cmd = "tar -czvf {cluster_tar} {bento_home}".format(
            cluster_tar=self.cluster_tgz,
            bento_home=self.bento_home
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


    def _run_kiji_job(self, cmd):
        cmd = "source {bento_home}/bin/kiji-env.sh; {cmd}".format(
            bento_home=self.bento_home, cmd=cmd)
        print(run(cmd))

    def _scan_table(self, uri):
        """ Scan this table and print out a couple of rows as a sanity check """
        cmd = 'kiji scan {kiji_uri}/{uri} --max-versions=10'.format(
            kiji_uri=self.kiji_uri,
            uri=uri)
        self._run_kiji_job(cmd)

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

            bento_dir_name = os.path.basename(
                self.bento_home if not self.bento_home.endswith("/") else self.bento_home[:-1])
            assert bento_dir_name != ""

            # Delete what was there before.
            fabric.api.run("rm -rf %s" % bento_dir_name)

            # Untar it!
            fabric.api.run("tar -zxvf %s" % cluster_tar_name)

        fabric.api.execute(copy_bento, host=self.remote_host)


    def _run_actions(self):
        """ Run whatever actions the user has specified """

        if "install-bento" in self.actions:
            self._do_action_install_bento()

        if "copy-bento" in self.actions:
            self._do_action_copy_bento()

    def go(self, args):
        try:
            self._parse_options(args)
            self._run_actions()
        finally:
            fabric.network.disconnect_all()


if __name__ == "__main__":
    InfraManager().go(sys.argv[1:])
