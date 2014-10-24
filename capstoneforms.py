from flask.ext.wtf import Form
from wtforms import StringField, SelectField, FileField
from wtforms.validators import DataRequired
import csv

with open('data/industrycodes.csv') as industrydata:
    person = list(csv.reader(industrydata))

one_list = map(lambda x: x[2], person)
INDUSTRY = zip(one_list,one_list)
INDUSTRY.pop(-1)



class Person(Form):
    resume = FileField()
    title = StringField("Please enter an appropriate title for your next job", validators=[DataRequired()])

    size = SelectField("What is the optimal company size for you?", choices = [
        ('1', 'Under 50 Employees'), ('2', '50 - 200 Employees'), ('3', '200 - 1000 Employees'), ('4', '1000 - 500 Employees'),
    ('5', 'Over 5000 Employees') ], default=1)

    age = SelectField("What is the optimal company age for you?", choices = [
        ('1', 'Under 3 Years'), ('2', '3 - 10 Years'), ('3', '10 - 30 Years'), ('4', '30 - 50 Years'), ('5', 'Over 50 Years')], default=1)

    industry = SelectField("Please choose the ideal industry for your next job", choices = INDUSTRY)

    type = SelectField("What is the optimal company type for you?", choices = [('1', 'Privately Held'),
    ('2', 'Government Agency'), ('3', 'Educational or Non-Profit'), ('4', 'Public Company')])

    workplace1 = StringField("Please enter your last company", validators=[DataRequired()])
    workrating1 = SelectField("Please enter your last company", choices = [
        ('1', 'Aweful'), ('2', 'Bearable'), ('3', 'OK'), ('4', 'Good'), ('5', 'Great! Would return')], default=3)

    workplace2 = StringField("Please enter your last company", validators=[DataRequired()])
    workrating2 = SelectField("Please enter your last company", choices = [
        ('1', 'Aweful'), ('2', 'Bearable'), ('3', 'OK'), ('4', 'Good'), ('5', 'Great! Would return')], default=3)

    workplace3 = StringField("Please enter your last company", validators=[DataRequired()])
    workrating3 = SelectField("Please enter your last company", choices = [
        ('1', 'Aweful'), ('2', 'Bearable'), ('3', 'OK'), ('4', 'Good'), ('5', 'Great! Would return')], default=3)
