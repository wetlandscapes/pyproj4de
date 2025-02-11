# Using pixi

Notes on using [`pixi`](https://pixi.sh/). The pixi documentation still seems a bit sparse (early days), so adding some insights into how I can do a couple of useful things, relative to this project.


## Create a development environment

Pattern:

```python
pixi project environment add
    --feature <env name> \
    --solve-group <grp name> \
    <env name>
# ...Then update other environments, if necessary
```

Example:

```python
# Step 1: Create dev environment
pixi project environment add --f dev --solve-group base dev
# Step 2: Ensure default/production environment are solved together
pixi project environment add --force --solve-group base default
```

> [!NOTE]
> Using `--solve-group` to ensure both the `dev` and `default` environments have the same underlying packages; the `dev` environment is a strict superset of the `default` environment.


## Add a library (dependency) to a specific environment

Pattern:

```python
pixi add --feature <environment name> <package(s)>
```

Example: Adding [`ruff`](https://docs.astral.sh/ruff/) to the `dev` environment.

```python
pixi add -f dev ruff
```

## Activate an environment

Pattern:

```python
pixi shell --environment <environment name>
```

Example:

```python
pixi shell -e dev
```

> [!NOTE] 
> The defualt environment will be used when first "shelling" into pixi. That is, from the command line, `pixi shell` will enter whatever the default environment is.