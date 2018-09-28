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

@task(help={'pytest': "Arguments to pass to pytest running in the container."})
def test(ctx, pytest=''):
    'Runs the test suite.  User can specifiy pytest options to run specific tests.'
    ctx.run('docker-compose run app pytest {}'.format(pytest))

@task
def lint(ctx):
    'Checks code quality with pylint'
    ctx.run('docker-compose run app pylint --rcfile pemi/.pylintrc pemi')
    ctx.run('docker-compose run app pylint --rcfile tests/.pylintrc tests')


@task
def doc_generate(ctx):
    'Generates documents locally and writes to docs/_build/html'
    ctx.run('docker-compose run -w /app/docs --rm app sphinx-build -E . _build/html')

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
    ctx.run('pip-compile requirements/base.in')
    ctx.run('pip-compile requirements/doc.in')
    ctx.run('pip-compile requirements/all.in')

@task
def pip_compile_upgrade(ctx):
    'Upgrate pip resources.  This should only be run in the container.'
    ctx.run('pip-compile -U requirements/base.in')
    ctx.run('pip-compile -U requirements/doc.in')
    ctx.run('pip-compile -U requirements/all.in')
