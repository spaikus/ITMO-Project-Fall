#pragma once

#include <iostream>

#include <generators/generate_data.hpp>

// TODO: really should move away from static typing

IOFIELD(uint64_t, id);
IOFIELD(uint64_t, uniform);
IOFIELD(uint64_t, norm);
IOFIELD(uint64_t, ifiller11);
IOFIELD(uint64_t, ifiller12);
IOFIELD(uint64_t, ifiller13);
IOFIELD(uint64_t, ifiller14);
IOFIELD(uint64_t, ifiller21);
IOFIELD(uint64_t, ifiller22);
IOFIELD(uint64_t, ifiller23);
IOFIELD(uint64_t, ifiller24);
IOFIELD(uint64_t, ifiller31);
IOFIELD(uint64_t, ifiller32);
IOFIELD(uint64_t, ifiller33);
IOFIELD(uint64_t, ifiller34);
IOFIELD(uint64_t, ifiller41);
IOFIELD(uint64_t, ifiller42);
IOFIELD(uint64_t, ifiller43);
IOFIELD(uint64_t, ifiller44);
IOFIELD(uint64_t, ifiller51);
IOFIELD(uint64_t, ifiller52);
IOFIELD(uint64_t, ifiller53);
IOFIELD(uint64_t, ifiller54);
IOFIELD(uint64_t, ifiller61);
IOFIELD(uint64_t, ifiller62);
IOFIELD(uint64_t, ifiller63);
IOFIELD(uint64_t, ifiller64);
IOFIELD(uint64_t, ifiller71);
IOFIELD(uint64_t, ifiller72);
IOFIELD(uint64_t, ifiller73);
IOFIELD(uint64_t, ifiller74);
IOFIELD(uint64_t, ifiller81);
IOFIELD(uint64_t, ifiller82);
IOFIELD(uint64_t, ifiller83);
IOFIELD(uint64_t, ifiller84);

using RowSingle = io::Row<IOFieldNuniform>;

using RowFew = io::Row<IOFieldNid, IOFieldNuniform, IOFieldNnorm>;

using RowWide = io::Row<IOFieldNid, IOFieldNuniform, IOFieldNnorm,
                        IOFieldNifiller11, IOFieldNifiller12, IOFieldNifiller13,
                        IOFieldNifiller14, IOFieldNifiller21>;

using RowWideWide = io::Row<
    IOFieldNuniform, IOFieldNifiller12, IOFieldNifiller13, IOFieldNifiller14,
    IOFieldNifiller21, IOFieldNifiller22, IOFieldNifiller23, IOFieldNifiller24,
    IOFieldNifiller31, IOFieldNifiller32, IOFieldNifiller33, IOFieldNifiller34,
    IOFieldNifiller41, IOFieldNifiller42, IOFieldNifiller43, IOFieldNifiller44,
    IOFieldNifiller51, IOFieldNifiller52, IOFieldNifiller53, IOFieldNifiller54,
    IOFieldNifiller61, IOFieldNifiller62, IOFieldNifiller63, IOFieldNifiller64,
    IOFieldNifiller71, IOFieldNifiller72, IOFieldNifiller73, IOFieldNifiller74,
    IOFieldNifiller81, IOFieldNifiller82, IOFieldNifiller83, IOFieldNifiller84>;

const auto kRowSingleGenSchema = std::make_tuple(generators::FieldToGenerate(
    "uniform", std::uniform_int_distribution<uint64_t>()));

const auto kRowFewGenSchema = std::make_tuple(
    generators::FieldToGenerate("id", generators::AutoIncrement<uint64_t>()),
    generators::FieldToGenerate("uniform",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate(
        "norm", generators::CastTo<uint64_t>(
                    std::normal_distribution<>(10000000, 10000000))));

const auto kRowWideGenSchema = std::make_tuple(
    generators::FieldToGenerate("id", generators::AutoIncrement<uint64_t>()),
    generators::FieldToGenerate("uniform",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate(
        "norm", generators::CastTo<uint64_t>(
                    std::normal_distribution<>(10000000, 10000000))),
    generators::FieldToGenerate("ifiller11",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller12",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller13",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller14",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller21",
                                std::uniform_int_distribution<uint64_t>()));

const auto kRowWideWideGenSchema = std::make_tuple(
    generators::FieldToGenerate("uniform",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller12",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller13",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller14",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller21",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller22",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller23",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller24",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller31",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller32",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller33",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller34",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller41",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller42",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller43",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller44",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller51",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller52",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller53",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller54",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller61",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller62",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller63",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller64",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller71",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller72",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller73",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller74",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller81",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller82",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller83",
                                std::uniform_int_distribution<uint64_t>()),
    generators::FieldToGenerate("ifiller84",
                                std::uniform_int_distribution<uint64_t>()));
