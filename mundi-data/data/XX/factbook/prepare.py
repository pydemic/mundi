import json
import re
from dataclasses import dataclass
from numbers import Number
from pathlib import Path
from typing import Optional, Union

import sidekick.api as sk
from bs4 import BeautifulSoup, NavigableString as BSStr
from sidekick import placeholder as _

import mundi
from mundi.plugins.factbook import COUNTRIES


def prettify(html):
    soup = BeautifulSoup(html)
    return soup.prettify()


decoder = json.JSONDecoder(strict=False)
NUMERIC = re.compile(r'-?[0-9]+(,[0-9]{3})*(\.[0-9]+)?')
NOT_GIVEN = object()


class Region:
    DATABASE_PATH = Path(__file__).parent / 'book' / 'json'
    SECTIONS = frozenset({
        'introduction', 'geography', 'society', 'government', 'economy',
        'energy', 'communications', 'military', 'transportation', 'issues',
    })
    SECTION_REFS = {
        'society': 'people-and-society',
        'military': 'military-and-security',
        'issues': 'transnational-issues',
    }
    _scrappers = {}
    _parsers = {}
    _empty = {}

    path: Path = sk.lazy(lambda r: r.DATABASE_PATH / f'{r.code}.json')
    json: dict = sk.lazy(lambda r: decoder.decode(r.path.read_text('utf-8')))
    name = sk.lazy(_.json['name'])
    html = sk.property(_.json['html'])
    soup: BeautifulSoup = sk.lazy(lambda r: BeautifulSoup(r.html, 'lxml'))
    html_pretty = sk.alias('html', transform=prettify)

    # Quick access to sections
    geography = sk.lazy(_.scrap_section(('geography',)))
    society = sk.lazy(_.scrap_section(('society',)))
    government = sk.lazy(_.scrap_section(('government',)))
    economy = sk.lazy(_.scrap_section(('economy',)))
    energy = sk.lazy(_.scrap_section(('energy',)))
    communications = sk.lazy(_.scrap_section(('communications',)))
    military = sk.lazy(_.scrap_section(('military',)))
    transportation = sk.lazy(_.scrap_section(('transportation',)))
    issues = sk.lazy(_.scrap_section(('issues',)))
    introduction = sk.lazy(_.scrap_section(('introduction',)))

    @classmethod
    def register_overrides(cls):
        for k in dir(cls):
            if k.startswith('scrap__'):
                method = getattr(cls, k)
                cls._scrappers[tuple(k.split('__')[1:])] = method
            elif k.startswith('parse__'):
                method = getattr(cls, k)
                cls._parsers[tuple(k.split('__')[1:])] = method
            elif k.startswith('empty__'):
                method = getattr(cls, k)
                cls._empty[tuple(k.split('__')[1:])] = method

        cls._empty.update({
            ('communications', 'telephones_fixed_lines'): empty_truncate_at_small_values,
            ('communications',
             'broadband_fixed_subscriptions'): empty_truncate_at_small_values,
            ('communications', 'telephones_fixed_lines',
             'subscriptions_per_100_inhabitants'): empty_truncate_at_small_values,
            ('communications', 'broadband_fixed_subscriptions',
             'subscriptions_per_100_inhabitants'): empty_truncate_at_small_values,
            ('geography', 'maritime_claims', 'exclusive_economic_zone'): empty_void,
            ('geography', 'maritime_claims',
             'exclusive_fishing_zone'): empty_average_over_range,
            ('geography', 'maritime_claims', 'territorial_sea'): empty_void,
            ('society', 'hiv_aids_adult_prevalence_rate'): empty_truncate_at_small_values,
            ('society', 'hiv_aids_deaths'): empty_truncate_at_small_values,
            ('society',
             'hiv_aids_people_living_with_hiv_aids'): empty_truncate_at_small_values,
        })

    def __init__(self, code):
        self.code = code.lower()
        self._processed = {}

    def __getstate__(self):
        return self.code

    def __setstate__(self, code):
        self.__init__(code)

    def __repr__(self):
        cls = type(self).__name__
        return f'{cls}({self.code!r})'

    def all(self, keys=SECTIONS):
        return {key: self.scrap_section((key,)) for key in keys}

    def section_tag(self, category) -> BeautifulSoup:
        section = self.soup.select_one(f'#{category}-category-section')
        if section is None:
            raise ValueError(f'category not found in {self.code}: {category}')
        return section

    #
    # Generic scrap functions
    #
    def scrap_section(self, path) -> dict:
        section, = path
        if section in self._processed:
            return self._processed[section]

        ref = self.SECTION_REFS.get(section, section)
        try:
            soup = self.section_tag(ref)
        except ValueError:
            return {}

        out = self._processed[section] = {}
        for subsection in soup.children:
            if subsection.attrs['id'].startswith('field-anchor'):
                continue

            key = subsection.attrs['id'].partition('-')[2].replace('-', '_')
            data = self.scrap_subsection((section, key), subsection)
            merge(out, data)

        return out

    def scrap_subsection(self, path: tuple, subsection) -> dict:
        try:
            method = self._scrappers[path]
        except KeyError:
            pass
        else:
            return method(self, subsection)

        body = {}
        key = path[-1]
        common = {'category_data', 'subfield'}

        for field in subsection.select('.subfield'):
            field_type, = set(field.attrs['class']) - common

            if field_type == 'numeric':
                part = self.scrap_numeric_field(path, field)
            elif field_type == 'text':
                part = self.scrap_text_field(path, field)
            elif field_type == 'grouped_subfield':
                part = self.scrap_grouped_subfield(path, field)
            elif field_type == 'historic':
                part = self.scrap_historic_field(path, field)
            else:
                raise ValueError(f'invalid field type: {field_type}')

            merge(body, part)

        if len(body) == 1 and None in body:
            return {key: body.pop(None)}
        if None in body:
            body['value'] = value = body.pop(None)
            if value == {}:
                del body['value']
        if body == {}:
            return {}
        return {key: body}

    def scrap_numeric_field(self, path, field):
        name = self.parse_field_name(self.extract_one(field, '.subfield-name'))
        date = self.extract_date(field)
        note = self.extract_note(field)
        number = self.extract_one(field, '.subfield-number')
        path = (*path, name) if name else path
        number = self.parse_number(path, number, note, date)
        return {name: number}

    def scrap_text_field(self, path, field):
        name = self.parse_field_name(self.extract_one(field, '.subfield-name'))
        date = self.extract_date(field)
        text = self.extract_text(field)
        path = (*path, name) if name else path
        return {name: self.parse_text(path, text, date)}

    def scrap_grouped_subfield(self, path, field):
        raise RuntimeError

    def scrap_historic_field(self, path, field):
        # It seems that historic fields are mostly numeric
        if field.select_one('.subfield-number'):
            return self.scrap_numeric_field(path, field)

        # Some fields are empty and have no number nor notes, only a date
        note = self.extract_note(field)
        date = self.extract_date(field)
        if not note:
            return {}
        return {path[-1]: self.parse_number(path, None, note, date)}

    def scrap_grouped_subsection(self, path, field):
        groups = {}
        group = {}
        for child in field.children:
            group_tag = self.parse_field_name(self.extract_one(child, '.subfield-group'))
            if group_tag:
                groups[group_tag] = group = {}
            value = self.scrap_numeric_field(path, child)
            merge(group, value)

        return {path[-1]: groups}

    #
    # Specialized scrap functions
    #
    def scrap__geography__land_use(self, field):
        out = {}
        for child in field.children:
            if 'numeric' in child.attrs['class']:
                value = self.scrap_numeric_field(('geography', 'land_use'), child)
                merge(out, value)
                continue

            while child.select_one('.subfield-number'):
                value = self.scrap_numeric_field(('geography', 'land_use'), child)
                merge(out, value)
        return {'land_use': out}

    def scrap__society__drinking_water_source(self, field):
        path = ('society', 'drinking_water_source')
        return self.scrap_grouped_subsection(path, field)

    def scrap__society__sanitation_facility_access(self, field):
        path = ('society', 'sanitation_facility_access')
        return self.scrap_grouped_subsection(path, field)

    def empty__geography__maritime_claims__continental_shelf(self, note, date):
        if m := NUMERIC.match(note):
            value = m.group()
            tail = note[len(value):].strip()
            if tail.startswith('-m depth') or tail.startswith('m depth'):
                return self.parse_number((), value + ' m depth', note, date)
            elif tail.startswith('nm'):
                return self.parse_number((), value + ' nm', note, date)
        return void(note, date)

    def empty__transportation__waterways(self, note, date):
        return read_annotated_number(0, note, date)

    def empty__society__population(self, note, date):
        return read_annotated_number(0, note, date)

    def empty__military__military_expenditures(self, note, date):
        if self.code == 'eg':
            # Special case entry in Egyptian page.
            return self.parse_number((), 2.5, note, date)
        raise NotImplemented

    #
    # Auxiliary methods for extracting and parsing elements
    #
    def extract_one(self, field, selector):
        tag = field.select_one(selector)
        if tag:
            tag.extract()
            return tag.text.strip()

    def extract_date(self, field):
        date = self.extract_one(field, '.subfield-date') or None
        if date:
            return clear_parens(date)

    def extract_note(self, field):
        note = self.extract_one(field, '.subfield-note') or None
        if note:
            return clear_parens(note)

    def extract_text(self, field):
        value = ''
        for child in list(field.children):
            if isinstance(child, BSStr):
                child.extract()
                value += str(child)
            elif child.name == 'br':
                child.extract()
                value += '\n'
            elif child.name == 'p':
                child.extract()
                value += f'\n{child.text}\n'
        return value.strip(' \n/:') or None

    def parse_field_name(self, st):
        if not st:
            return None
        return st.strip(': ').replace(' ', '_')

    def parse_number(self, path, value, note, date):
        if parser := self._parsers.get(path):
            value = parser(self, value, note, date)
            if value is NotImplemented:
                value = None
            else:
                return value

        if value is None:
            if note in ('NA', 'N/A', 'NA%', '$NA', 'NA cu m', 'NA bbl', 'NEGL'):
                return void(date=date)

            # Verify empty method handlers
            if method := self._empty.get(path):
                return method(self, note, date)

            # Sometimes the numeric data is inside a span.subfield-note tag, for
            # some reason
            try:
                return read_annotated_number(note, None, date)
            except ValueError:
                pass

            # Finally, we give up and print a debug message
            st.write({
                "country": f'{self.name} ({self.code})',
                "note": note,
                "date": date,
                "numeric_method": method,
            })
            return void(note, date)
        return read_annotated_number(value, note, date)

    def parse_text(self, path, value, date):
        if parser := self._parsers.get(path):
            return parser(self, value, date)
        return value


class Country(Region):
    mundi = sk.lazy(lambda self: mundi.region(self.code))


class NumericMixin:
    note: Optional[str]
    date: Optional[str]
    unit: Optional[str]

    def __new__(cls, value, unit=None, note=None, date=None):
        new = super().__new__(cls, value)
        new.note = note
        new.date = date
        new.unit = unit
        return new

    def __repr__(self):
        return f'{type(self).__name__}({self})'


class Int(NumericMixin, int):
    ...


class Float(NumericMixin, float):
    ...


@dataclass()
class Void:
    note: Optional[str] = None
    date: Optional[str] = None


def void(note=None, date=None):
    if note is None and date is None:
        return None
    return Void(note, date)


#
# Parsers
#
def read_simple_number(value: Union[str, Number]):
    if isinstance(value, Number):
        return value

    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass

    if value.endswith('%'):
        return read_simple_number(value[:-1]) / 100

    if value.startswith('$'):
        return read_simple_number(value[1:])

    if value.startswith('-'):
        return -read_simple_number(value[1:])

    raise ValueError(value)


def read_annotated_number(value: Union[str, Number], note: Optional[str],
                          date: Optional[str]):
    if isinstance(value, Number):
        num, unit = value, None
    else:
        num, _, unit = value.strip().partition(' ')
        num = read_simple_number(num.replace(',', ''))
    if unit or note or date:
        if isinstance(num, int):
            return Int(num, note=note, date=date, unit=unit)
        return Float(num, note=note, date=date, unit=unit)
    return num


#
# Empty value handlers
#
def empty_void(region, note, date):
    return void(note, date)


def empty_average_over_range(region, note, date):
    if m := NUMERIC.match(note):
        start = m.group()
        end = region.parse_number((), note[len(start) + 1], note, date)
        a, b = map(float, [start, end])
        return Float((a + b) / 2, note=note, date=date)
    return NotImplemented


def empty_truncate_at_small_values(region, note, date):
    if note.startswith('<'):
        # We don't known the actual value. The heuristic is to select the
        # midpoint between 0 and maximum.
        value = note[1:]
    elif note.startswith('less than'):
        value = note[9:].strip()
    else:
        return NotImplemented
    return region.parse_number((), value, note, date) / 2


#
# Utilities
#


def merge(parent, child):
    parent.update(child)


def clear_parens(st):
    if st.startswith('(') and st.endswith(')'):
        return st[1:-1]
    return st


def countries():
    return [Country(code) for code in COUNTRIES]


def fetch_all():
    return [r.all() for r in countries()]


def save_db():
    with open('db.pkl', 'wb') as fd:
        pickle.dump(fetch_all(), fd)


def load_db():
    with open('db.pkl', 'rb') as fd:
        return pickle.load(fd)


def paths(json, path=''):
    if isinstance(json, dict):
        for k, v in json.items():
            yield from paths(v, f'{path}/{k}')
    elif isinstance(json, (list, tuple)):
        for i, v in enumerate(json):
            yield from paths(v, f'{path}/{i}')
    else:
        yield path


def iter_paths(value, path=()):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from iter_paths(v, (*path, k))
    elif isinstance(value, (list, tuple)):
        for i, v in enumerate(value):
            yield from iter_paths(v, (*path, i))
    else:
        yield path, value

def safe_key(k):
    if isinstance(k, tuple):
        return '/'.join(map(safe_key, k))
    return str(k)


def safe_json(value):
    if isinstance(value, dict):
        return {safe_key(k): safe_json(v) for k, v in value.items()}
    elif isinstance(value, (list, tuple, set, frozenset)):
        return [safe_json(v) for v in value]
    elif isinstance(value, (str, int, float, bool, type(None))):
        return value
    return value


Region.register_overrides()




if __name__ == '__main__':
    import streamlit as st
    import pickle
    from collections import Counter, defaultdict


    @st.cache
    def all_paths():
        db = load_db()
        lst = set()
        for r in db:
            lst.update(paths(r))
        return tuple(sorted(lst))


    @st.cache
    def count_paths():
        db = load_db()
        counter = Counter()
        for r in db:
            counter.update(paths(r))
        return dict(counter.most_common())


    @st.cache
    def types():
        tree = defaultdict(Counter)
        for path, v in iter_paths(load_db()):
            tree[path[1:]][type(v)] += 1
        return tree


    @st.cache
    def safe_types():
        return safe_json(types())

    def inv_list(lst: list) -> dict:
        if isinstance(lst, dict):
            return lst
        out = defaultdict(list)
        try:
            for x in lst:
                for k, v in x.items():
                    out[k].append(v)
        except AttributeError:
            return lst
        return out

    def lens(db, key=0):
        if isinstance(db, list):
            db = inv_list(db)
        curr = db

        while hasattr(curr, 'keys'):
            opt = st.selectbox('Select', ['*', *curr.keys()], key=f'lens-{key}')
            key += 1
            if opt == '*':
                break
            curr = inv_list(curr[opt])
        else:
            if (opt := st.selectbox('Show', ['array', 'bar'])) == 'array':
                st.write(curr)
            elif opt == 'bar':
                st.bar_chart(curr)
            return

        if len(curr) > 50 and not st.checkbox('Show all'):
            st.write([...])
        else:
            st.write(curr)

    # save_db()
    lens(load_db())
