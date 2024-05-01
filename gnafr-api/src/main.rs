use std::path::PathBuf;

use actix_web::{error, get, web, App, HttpResponse, HttpServer, Result};
use clap::Parser;
use serde::Serialize;
use sqlx::{query_as, SqlitePool};

pub const RANGE: f64 = 0.0001;

#[derive(Debug, Parser)]
struct Args {
    #[arg(short, long)]
    database_path: Option<String>,
    #[arg(short, long)]
    port: Option<u16>,
}

#[get("/{lat}/{lon}")]
async fn locate(path: web::Path<(f64, f64)>, pool: web::Data<SqlitePool>) -> Result<HttpResponse> {
    let (lat, lon) = path.into_inner();
    let mut range = RANGE;
    loop {
        let min_lat = lat - range;
        let max_lat = lat + range;
        let min_lon = lon - range;
        let max_lon = lon + range;

        let choices = query_as!(
            DBAddress,
            "select * from address where latitude between ? and ? and longitude between ? and ?",
            min_lat,
            max_lat,
            min_lon,
            max_lon,
        )
        .fetch_all(&**pool)
        .await
        .map_err(|x| error::ErrorInternalServerError(x))?;

        if choices.len() == 0 {
            range *= 2.0;
            if range > 0.1 {
                break;
            } else {
                continue;
            }
        }

        let mut best = choices.get(0).unwrap();
        let mut best_distance = haversine_distance(lat, lon, best.latitude, best.longitude);
        for other in choices.iter().skip(1) {
            let distance = haversine_distance(lat, lon, other.latitude, other.longitude);
            if distance < best_distance {
                best = other;
                best_distance = distance;
            } else if distance == best_distance {
                for x in [&best.child_of, &best.alias_of] {
                    if let Some(x) = x {
                        if x == &other.id {
                            best = other;
                        }
                    }
                }
            }
        }
        return Ok(HttpResponse::Ok().json(best));
    }

    return Ok(HttpResponse::Ok().json(Option::<DBAddress>::None));
}

#[get("/{id}")]
async fn id(path: web::Path<String>, pool: web::Data<SqlitePool>) -> Result<HttpResponse> {
    let id = path.into_inner();
    let result = query_as!(DBAddress, "select * from address where id = ?", id)
        .fetch_optional(&**pool)
        .await
        .map_err(|x| error::ErrorInternalServerError(x))?;

    if let Some(result) = result {
        Ok(HttpResponse::Ok().json(result))
    } else {
        Ok(HttpResponse::NotFound().json(result))
    }
}

#[get("/")]
async fn copyright() -> HttpResponse {
    HttpResponse::Ok().body(
        "Licensed by Geoscape Australia under the Open G-NAF Core End User Licence Agreement.",
    )
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let pool = SqlitePool::connect(
        args.database_path
            .as_deref()
            .unwrap_or("../gnafr-db/gnafr.db"),
    )
    .await
    .expect("failed to open db");

    let port = args.port.unwrap_or(8000);
    eprintln!("Starting on 127.0.0.1:{port}");
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .service(locate)
            .service(id)
            .service(copyright)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}

#[derive(Debug, Serialize)]
struct DBAddress {
    id: String,
    display: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    site_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    building_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    flat_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    flat_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    level_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    level_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    number_first: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    number_last: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lot_number: Option<String>,
    street_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    street_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    street_suffix: Option<String>,
    locality: String,
    state: String,
    postcode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    alias_of: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    child_of: Option<String>,
    latitude: f64,
    longitude: f64,
}

fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const MEAN_EARTH_RADIUS: f64 = 6371.0; // in kilometers

    let two = 2.0;
    let theta1 = lat1.to_radians();
    let theta2 = lat2.to_radians();
    let delta_theta = (lat2 - lat1).to_radians();
    let delta_lambda = (lon2 - lon1).to_radians();

    let a = (delta_theta / two).sin().powi(2)
        + theta1.cos() * theta2.cos() * (delta_lambda / two).sin().powi(2);
    let c = two * a.sqrt().asin();

    MEAN_EARTH_RADIUS * c
}
