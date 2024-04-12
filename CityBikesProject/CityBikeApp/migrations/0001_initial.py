# Generated by Django 4.2.11 on 2024-04-11 19:33

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Bike',
            fields=[
                ('bike_id', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('bike_type', models.CharField(choices=[('electric', 'Electric'), ('classic', 'Classic'), ('unknown', 'Unknown')], default='unknown', max_length=255)),
            ],
        ),
        migrations.CreateModel(
            name='File',
            fields=[
                ('file_name', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('file_path', models.CharField(max_length=255)),
                ('last_modified', models.DateTimeField()),
                ('size', models.FloatField()),
            ],
        ),
        migrations.CreateModel(
            name='Station',
            fields=[
                ('station_id', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('station_name', models.CharField(max_length=255)),
                ('lat', models.FloatField()),
                ('lon', models.FloatField()),
            ],
        ),
        migrations.CreateModel(
            name='Ride',
            fields=[
                ('ride_id', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('started_at', models.DateTimeField()),
                ('ended_at', models.DateTimeField()),
                ('rider_birth_year', models.IntegerField(blank=True, null=True)),
                ('rider_gender', models.IntegerField(blank=True, choices=[(0, 'Unknown'), (1, 'Male'), (2, 'Female')], default=0, null=True)),
                ('rider_member_or_casual', models.CharField(blank=True, max_length=255, null=True)),
                ('bike_id', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='CityBikeApp.bike')),
                ('end_station', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='rides_ended', to='CityBikeApp.station')),
                ('source_file', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='rides', to='CityBikeApp.file')),
                ('start_station', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='rides_started', to='CityBikeApp.station')),
            ],
        ),
    ]
