

-- TODO Com a exercici/motivació per validar 

[Richest 1% emit as much planet-heating pollution as two-thirds of humanity - Oxfam](https://www.oxfam.org/en/press-releases/richest-1-emit-much-planet-heating-pollution-two-thirds-humanity)
> It would take about 1,500 years for someone in the bottom 99 percent to produce as much carbon as the richest billionaires do in a year.  

> The carbon emissions of richest 1 percent are set to be 22 times greater than the level compatible with the 1.5°C goal of the Paris Agreement in 2030.

```shell
pip install pytest-json-report mypy[reports]

pytest --json-report --json-report-summary --json-report-file=.pytest.json --cov=bdi_api --cov-report json:.coverage.json -q tests/

mypy bdi_api/s1 --txt-report .mypy --linecount-report . 

```
https://mypy.readthedocs.io/en/stable/command_line.html?highlight=output#report-generation

files: 
* .coverage.json
* 