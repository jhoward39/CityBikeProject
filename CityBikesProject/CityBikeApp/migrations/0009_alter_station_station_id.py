# Generated by Django 4.2.11 on 2024-04-12 10:14

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('CityBikeApp', '0008_alter_bike_bike_id_alter_ride_ride_id'),
    ]

    operations = [
        migrations.AlterField(
            model_name='station',
            name='station_id',
            field=models.AutoField(primary_key=True, serialize=False),
        ),
    ]