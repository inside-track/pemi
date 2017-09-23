from invoke import task

@task
def package(ctx):
    '''
    Package distribution to upload to PyPI
    '''
    ctx.run('rm -rf dist')
    ctx.run('python setup.py sdist')

@task
def package_deploy(ctx):
    '''
    Deploy package to PyPI
    '''
    ctx.run('twine upload dist/*')

@task
def compile_requirements(ctx):
    '''
    Compile Python requirements without upgrading.
    Docker images need to be rebuilt after running this (inv build).
    '''
    ctx.run('docker-compose run app inv pip-compile')

@task
def upgrade_requirements(ctx):
    '''
    Compile Python requirements with upgrading.
    Docker images need to be rebuilt after running this (inv build).
    '''
    ctx.run('docker-compose run app inv pip-compile-upgrade')

@task
def build(ctx):
    'Build docker images'
    ctx.run('docker-compose build')

@task
def up(ctx):
    'Start up development environment'
    ctx.run('docker-compose up -d')

@task
def down(ctx):
    'Shut down development environment'
    ctx.run('docker-compose down')

@task(down, up)
def restart(ctx):
    'Restart development environment'

@task
def logs(ctx):
    'Follow docker logs'
    ctx.run('docker-compose logs -f')

@task(help={'nose2': "Arguments to pass to nose2 running in the container."})
def test(ctx, nose2=''):
    'Runs the test suite.  User can specifiy nose2 options to run specific tests.'
    ctx.run('docker-compose run app nose2 {}'.format(nose2))

@task
def ps(ctx):
    'View environment containers'
    ctx.run('docker-compose ps')

@task
def shell(ctx):
    'Start a shell running in the app container'
    ctx.run('docker-compose run --rm app /bin/bash')

@task
def pip_compile(ctx):
    'Compile pip resources.  This should only be run in the container.'
    ctx.run('pip-compile requirements.in')

@task
def pip_compile_upgrade(ctx):
    'Upgrate pip resources.  This should only be run in the container.'
    ctx.run('pip-compile -U requirements.in')
