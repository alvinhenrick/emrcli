import functools
import pathlib
from pathlib import Path

import click
import username

from aws.utils.common.sampleconfig import emrcliconfig
from aws.utils.emr.emr import HandleEMRCommands


def common_params(func):
    @click.option('--config', default='emrcliconfig.yaml',
                  help='User config file.The default filename expected is emrcliconfig.yaml in current directory.')
    @click.option('--profile', default=None, help="The AWS Cli profile.The default will be used by default.")
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@click.group()
@click.pass_context
def cli(ctx):
    if ctx.obj is None:
        ctx.obj = {}


@cli.command()
@click.argument("env-name")
def init(env_name):
    try:
        path = "emrcli_{}".format(env_name)

        pathlib.Path(path).mkdir()

        emr_dir = Path(__file__).parent.joinpath('emr')

        for file_path in emr_dir.glob('*.json'):
            pathlib.Path(path).joinpath(file_path.name).write_text(file_path.read_text())

        pathlib.Path(path).joinpath("emrcliconfig.yaml").write_text(emrcliconfig.format(env_name, username()))

    except OSError:
        print("Creation of the directory %s failed" % path)
    else:
        print("Successfully created the directory %s" % path)


@cli.command()
@common_params
@click.pass_context
def create(ctx, config, profile):
    HandleEMRCommands(config, profile_name=profile).create()


@cli.command()
@common_params
@click.option('--keep_alive', is_flag=True, default=False, prompt="Do you want to keep cluster alive ?")
@click.option('-d', '--date', default=None, help="The user entered date otherwise todays date will be used.")
@click.option('--step_name', default=None, help="The steps to execute comma separated.")
@click.option('--step_idx', default=None, help="The step to execute array slice index.")
@click.option('--custom_vars', default=None, help="The custom variable , separated with key=value")
@click.pass_context
def create_with_steps(ctx, config, profile, keep_alive, date, step_name, step_idx, custom_vars):
    if step_name is not None and step_idx is not None:
        print('You can use --step_name or --step_idx not both.')
        raise click.Abort()

    local_custom_vars = get_custom_vars(custom_vars)

    HandleEMRCommands(config, with_steps=True, profile_name=profile, overwrite_auto_terminate=True) \
        .create(
        keep_alive=keep_alive, user_date=date, user_step_name=step_name, user_step_idx=step_idx,
        user_custom_vars=local_custom_vars)


@cli.command()
@common_params
@click.option('-c', '--cid', default=None, help="The the cluster id.This will use user supplied cluster id")
@click.option('-d', '--date', default=None, help="The user entered date otherwise todays date will be used.")
@click.option('--step_name', default=None, help="The steps to execute comma separated.")
@click.option('--step_idx', default=None, help="The step to execute array slice index.")
@click.option('--custom_vars', default=None, help="The custom variable , separated with key=value")
@click.pass_context
def submit_steps(ctx, config, profile, cid, date, step_name, step_idx,
                 custom_vars):
    if step_name is not None and step_idx is not None:
        print('You can use --step_name or --step_idx not both.')
        raise click.Abort()

    local_custom_vars = get_custom_vars(custom_vars)

    HandleEMRCommands(config, with_steps=True, profile_name=profile, user_job_flow_id=cid).add_steps(
        user_date=date, user_step_name=step_name, user_step_idx=step_idx,
        user_custom_vars=local_custom_vars)


@cli.command()
@common_params
@click.option('-c', '--cid', default=None, help="The the cluster id.This will use user supplied cluster id")
@click.pass_context
def terminate(ctx, config, profile, cid):
    if click.confirm('Are you sure you want to terminate cluster?', abort=True):
        HandleEMRCommands(config, profile_name=profile, user_job_flow_id=cid).terminate()


@cli.command()
@common_params
@click.option('-p', '--pem', default=None,
              help="The pem file to use.This will override settings used in emrcliconfig.yaml")
@click.option('-c', '--cid', default=None, help="The the cluster id.This will use user supplied cluster id")
@click.pass_context
def tunnel(ctx, config, profile, pem, cid):
    HandleEMRCommands(config, profile_name=profile, user_job_flow_id=cid).tunnel(user_pem=pem)


@cli.command()
@common_params
@click.option('-p', '--pem', default=None,
              help="The pem file to use.This will override settings used in emrcliconfig.yaml")
@click.option('-c', '--cid', default=None, help="The the cluster id.This will use user supplied cluster id")
@click.pass_context
def ssh(ctx, config, profile, pem, cid):
    HandleEMRCommands(config, profile_name=profile, user_job_flow_id=cid).ssh(user_pem=pem)


@cli.command()
@common_params
@click.option('-p', '--pem', default=None,
              help="The pem file to use.This will override settings used in emrcliconfig.yaml")
@click.option('-c', '--cid', default=None, help="The the cluster id.This will use user supplied cluster id")
@click.option('-s', '--script', default=None,
              help="The shell script to run.This will override settings used in emrcliconfig.yaml")
@click.option('-q', '--quiet', is_flag=True, default=False,
              help="The shell script will run in quiet mode.")
@click.pass_context
def script_runner(ctx, config, profile, pem, cid, script, quiet):
    HandleEMRCommands(config, profile_name=profile, user_job_flow_id=cid) \
        .script_runner(user_pem=pem, user_script_name=script, quiet_mode=quiet)


@cli.command()
@common_params
@click.option('-p', '--pem', default=None,
              help="The pem file to use.This will override settings used in emrcliconfig.yaml")
@click.option('-c', '--cid', default=None, help="The the cluster id.This will use user supplied cluster id")
@click.option('-q', '--quiet', is_flag=True, default=False,
              help="The shell script will run in quiet mode.")
@click.pass_context
def install(ctx, config, profile, pem, cid, quiet):
    HandleEMRCommands(config, profile_name=profile, user_job_flow_id=cid).install(
        user_pem=pem,
        quiet_mode=quiet)


def get_custom_vars(str_custom_vars):
    if str_custom_vars is not None:
        local_custom_vars = dict()
        for item in str_custom_vars.split(","):
            local_custom_vars.update([item.split('=')])
        return local_custom_vars
    else:
        return None
