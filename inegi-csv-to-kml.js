const fs = require('fs');
const parse = require('csv-parse');
const transform = require('stream-transform');
const S = require('string');
const fieldMap = require('./field-map.js');

const inputPath = process.argv[2]; // 'inegi_small.csv';
const outputPath = process.argv[3]; // 'inegi_small.kml';

if(!inputPath || !outputPath) {
	console.error('Use: node inegi-csv-kml.js <input_cvs_file> <output_kml_file>');
	process.exit(0);
}

const stream = fs.createReadStream(inputPath);
const output = fs.createWriteStream(outputPath);
//const output = process.stdout; // terminal

const parser = parse({ rtrim: true, columns: fieldMap });
const transformer = transform((record, callback) => callback(null, createPlacemark(record, parser.count - 1)), { parallel: 10 });

function writeStream(inputStream, outputStream, title) {
	outputStream.write(`<?xml version="1.0" encoding="utf-8"?>\n<kml><Document><name>${ title }</name>\n`);
	inputStream
		.pipe(parser)
		.pipe(transformer)
		.pipe(outputStream, {end: false})
	;
	inputStream.on('end', () => {
		// Put this under a timeline to allow all the piping to finish
		// TODO find a more elegant way to do this...
		setTimeout(() => outputStream.end('</Document></kml>\n'), 100);
	});
}

function createPlacemark(data, index) {
	const name = S(d(data, 'nombre_unidad_economica')).escapeHTML().s;
	const details = S(createDescription(data)).escapeHTML().escapeHTML().s;
	return (`
		<Placemark>
			<name>${ name }</name>
			<description addr="0" color="55ff0000" ride_begin="0" ride_end="0" width="10.0">${ details }</description>
			<Point><coordinates>${ d(data, 'latitud') },${ d(data, 'longitud') },0</coordinates></Point>
		</Placemark>
	`).replace(/\t/g, ' ');
}

function createDescription(data) {
	return (`
		<style type="text/css">
			.tg {font-family:Arial, sans-serif;font-size:12px;border-collapse:collapse;border-spacing:0;border-color:#ccc;}
			.tg td {padding:8px 6px;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#ccc;color:#333;background:transparent;}
			.tg tr:nth-child(odd) {background-color:#f9f9f9;} .tg tr.alt {background-color:#f0f0f0;}
		</style>
		<table class="tg">
			<tr class="alt"><td colspan="2">
				<b>Dirección:</b>
				<br />

				${ d(data, 'tipo_vialidad') } 
				${ d(data, 'nombre_vialidad') } 
				${ d(data, 'numero_exterior_o_kilometro') + d(data, 'letra_exterior') }
				${ d(data, 'numero_interior') + d(data, 'letra_interior') }
				<br />

				${ d(data, 'tipo_asentamiento_humano') }
				${ d(data, 'nombre_asentamiento_humano') }
				${ d(data, 'tipo_centro_comercial') }
				${ d(data, 'corredor_centro_mercado') }
				${ d(data, 'numero_local') }
				${ d(data, 'codigo_postal') }
				<br />

				${ d(data, 'entidad_federativa') }
				${ d(data, 'municipio') }
				${ d(data, 'localidad') }
			</td></tr>
			${ field('Personal', d(data, 'descripcion_estrato_personal_ocupado')) }
			${ field('Teléfono', d(data, 'Número de teléfono')) }
			${ field('Correo', d(data, 'Correo electrónico')) }
			${ field('Sitio Web', d(data, 'Sitio en Internet')) }
			${ field('Fecha Incorporación', d(data, 'Fecha de incorporación al DENUE')) }
		</table>
	`);
}

function d(data, key) {
	let value = data[key];
	if(typeof value === 'undefined') value = '';
	const capitalized = S(value).capitalize().s.split(/\s/g).map(s => {
		return s === 'de' ? s : S(s).capitalize().s;
	});
	return capitalized.join(' ');
}

function field(name, value) {
	return `<tr><td><b>${ name }</b></td><td>${ value }</td></tr>`;
}



writeStream(stream, output, 'INEGI');
