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
def requirements_compile(ctx):
    '''
    Compile Python requirements without upgrading.
    Docker images need to be rebuilt after running this (inv build).
    '''

    ctx.run('pip-compile requirements/base.in')
    ctx.run('pip-compile requirements/doc.in')
    ctx.run('pip-compile requirements/all.in')

@task
def requirements_upgrade(ctx):
    '''
    Compile Python requirements with upgrading.
    Docker images need to be rebuilt after running this (inv build).
    '''

    ctx.run('pip-compile -U requirements/base.in')
    ctx.run('pip-compile -U requirements/doc.in')
    ctx.run('pip-compile -U requirements/all.in')

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

DEFAULT_TEST_OPTS = '-s -x -vv --tb=short --color=yes'
@task(help={
    'tests': 'Specify the test you want to run - e.g., tests/test_fields.py',
    'opts': 'Set the options passed to pytest (default: {})'.format(DEFAULT_TEST_OPTS),
    'ignore': 'Specify the tests you want to ignore - e.g., tests/test_spark_job.py',
})
def test(ctx, opts=DEFAULT_TEST_OPTS, tests='tests', ignore=None):
    'Runs the test suite.  User can specifiy pytest options to run specific tests.'

    if ignore:
        opts += ' --ignore={}'.format(ignore)
    ctx.run('docker-compose run app pytest {} {}'.format(opts, tests))


@task(help={
    'all': 'Run all linters (default)',
    'app': 'Just run the app linter (pemi)',
    'tests': 'Just run the tests linter',
    'files': 'Specify files -- eg., pemi/fields.py'
})
def lint(ctx, all=False, app=False, tests=False, files=None):
    'Checks code quality with pylint'
    all = all or not (app or tests)

    if all or app:
        opts = files or 'pemi'
        ctx.run('docker-compose run app pylint --rcfile pemi/.pylintrc {}'.format(opts))

    if all or tests:
        opts = files or 'tests'
        ctx.run('docker-compose run app pylint --rcfile tests/.pylintrc {}'.format(opts))


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
