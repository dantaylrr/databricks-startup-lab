# databricks-startup-lab

Read this thoroughly first before running anything in the lab.

# Setup instrcutions.

### Advice.

* **Use a non-prod or development workspace if possible**.
* **Do not use a catalog or schema that contains any of your production assets (data, volumes, models)**.
* **If you are about how to do anything, ask a solution architect**.

### Lab Installation.

There are two ways to setup the lab:

* **Manual setup** - if you already have your Unity catalog & schema setup, create a volume under the schema you want to do the lab, name it something like `c360`. Once you have done so, navigate to the `data/` directory in this repo, download all of the source data files & upload them under the same volume you have just created, be sure to mirror the same directory structure under the volume. You should have `events`, `orders`, `users` sub-directories after doing this successfully.
* **Automated setup** - navigate to the `config` file in the repository root directory, set the catalog & schema name that you want the lab to be ran under, once specified, navigate to `init-lab`, connect to serverless & hit run. This will go off and create the catalog & schema for you if it doesn't already exist (& providing you have the correct permissions to do so).

**IMPORTANT - do not use a production or critical catalog for any of this, although we do not drop any catalogs or schemas, we want to ensure that none of your assets are touched. If you are unsure, go down the manual setup route & do everything in the UI rather than automated.**