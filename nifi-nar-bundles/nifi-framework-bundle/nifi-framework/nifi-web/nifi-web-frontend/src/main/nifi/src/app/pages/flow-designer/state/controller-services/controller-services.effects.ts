/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import * as ControllerServicesActions from './controller-services.actions';
import {
    catchError,
    combineLatest,
    from,
    map,
    NEVER,
    Observable,
    of,
    switchMap,
    take,
    tap,
    withLatestFrom
} from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectControllerServiceTypes } from '../../../../state/extension-types/extension-types.selectors';
import { CreateControllerService } from '../../../../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { Client } from '../../../../service/client.service';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { EditControllerService } from '../../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';
import {
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    NewPropertyDialogRequest,
    NewPropertyDialogResponse,
    Property,
    PropertyDescriptor
} from '../../../../state/shared';
import { NewPropertyDialog } from '../../../../ui/common/new-property-dialog/new-property-dialog.component';
import { Router } from '@angular/router';
import { ExtensionTypesService } from '../../../../service/extension-types.service';
import { selectCurrentProcessGroupId, selectSaving } from './controller-services.selectors';
import { ControllerServiceService } from '../../service/controller-service.service';

@Injectable()
export class ControllerServicesEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private controllerServiceService: ControllerServiceService,
        private extensionTypesService: ExtensionTypesService,
        private dialog: MatDialog,
        private router: Router
    ) {}

    loadControllerServices$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.loadControllerServices),
            map((action) => action.request),
            switchMap((request) =>
                combineLatest([
                    this.controllerServiceService.getControllerServices(request.processGroupId),
                    this.controllerServiceService.getBreadcrumbs(request.processGroupId)
                ]).pipe(
                    map(([controllerServicesResponse, breadcrumbsResponse]) =>
                        ControllerServicesActions.loadControllerServicesSuccess({
                            response: {
                                processGroupId: breadcrumbsResponse.id,
                                controllerServices: controllerServicesResponse.controllerServices,
                                loadedTimestamp: controllerServicesResponse.currentTime,
                                breadcrumb: breadcrumbsResponse
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ControllerServicesActions.controllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    openNewControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openNewControllerServiceDialog),
                withLatestFrom(
                    this.store.select(selectControllerServiceTypes),
                    this.store.select(selectCurrentProcessGroupId)
                ),
                tap(([action, controllerServiceTypes, processGroupId]) => {
                    const dialogReference = this.dialog.open(CreateControllerService, {
                        data: {
                            controllerServiceTypes
                        },
                        panelClass: 'medium-dialog'
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.createControllerService
                        .pipe(take(1))
                        .subscribe((controllerServiceType) => {
                            this.store.dispatch(
                                ControllerServicesActions.createControllerService({
                                    request: {
                                        revision: {
                                            clientId: this.client.getClientId(),
                                            version: 0
                                        },
                                        processGroupId,
                                        controllerServiceType: controllerServiceType.type,
                                        controllerServiceBundle: controllerServiceType.bundle
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.createControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.createControllerService(request)).pipe(
                    map((response) =>
                        ControllerServicesActions.createControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ControllerServicesActions.controllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    createControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.createControllerServiceSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    navigateToEditService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.navigateToEditService),
                map((action) => action.id),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([id, processGroupId]) => {
                    this.router.navigate(['/process-groups', processGroupId, 'controller-services', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    openConfigureControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.openConfigureControllerServiceDialog),
                map((action) => action.request),
                withLatestFrom(this.store.select(selectCurrentProcessGroupId)),
                tap(([request, processGroupId]) => {
                    const serviceId: string = request.id;

                    const editDialogReference = this.dialog.open(EditControllerService, {
                        data: {
                            controllerService: request.controllerService
                        },
                        panelClass: 'large-dialog'
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty = (
                        existingProperties: string[],
                        allowsSensitive: boolean
                    ): Observable<Property> => {
                        const dialogRequest: NewPropertyDialogRequest = { existingProperties, allowsSensitive };
                        const newPropertyDialogReference = this.dialog.open(NewPropertyDialog, {
                            data: dialogRequest,
                            panelClass: 'small-dialog'
                        });

                        return newPropertyDialogReference.componentInstance.newProperty.pipe(
                            take(1),
                            switchMap((dialogResponse: NewPropertyDialogResponse) => {
                                return this.controllerServiceService
                                    .getPropertyDescriptor(request.id, dialogResponse.name, dialogResponse.sensitive)
                                    .pipe(
                                        take(1),
                                        map((response) => {
                                            newPropertyDialogReference.close();

                                            return {
                                                property: dialogResponse.name,
                                                value: null,
                                                descriptor: response.propertyDescriptor
                                            };
                                        })
                                    );
                            })
                        );
                    };

                    editDialogReference.componentInstance.getServiceLink = (serviceId: string) => {
                        return this.controllerServiceService.getControllerService(serviceId).pipe(
                            take(1),
                            map((serviceEntity) => {
                                return [
                                    '/process-groups',
                                    serviceEntity.component.parentGroupId,
                                    'controller-services',
                                    serviceEntity.id
                                ];
                            })
                        );
                    };

                    editDialogReference.componentInstance.createNewService = (
                        serviceRequest: InlineServiceCreationRequest
                    ): Observable<InlineServiceCreationResponse> => {
                        const descriptor: PropertyDescriptor = serviceRequest.descriptor;

                        // fetch all services that implement the requested service api
                        return this.extensionTypesService
                            .getImplementingControllerServiceTypes(
                                // @ts-ignore
                                descriptor.identifiesControllerService,
                                descriptor.identifiesControllerServiceBundle
                            )
                            .pipe(
                                take(1),
                                switchMap((implementingTypesResponse) => {
                                    // show the create controller service dialog with the types that implemented the interface
                                    const createServiceDialogReference = this.dialog.open(CreateControllerService, {
                                        data: {
                                            controllerServiceTypes: implementingTypesResponse.controllerServiceTypes
                                        },
                                        panelClass: 'medium-dialog'
                                    });

                                    return createServiceDialogReference.componentInstance.createControllerService.pipe(
                                        take(1),
                                        switchMap((controllerServiceType) => {
                                            // typically this sequence would be implemented with ngrx actions, however we are
                                            // currently in an edit session and we need to return both the value (new service id)
                                            // and updated property descriptor so the table renders correctly
                                            return this.controllerServiceService
                                                .createControllerService({
                                                    revision: {
                                                        clientId: this.client.getClientId(),
                                                        version: 0
                                                    },
                                                    processGroupId,
                                                    controllerServiceType: controllerServiceType.type,
                                                    controllerServiceBundle: controllerServiceType.bundle
                                                })
                                                .pipe(
                                                    take(1),
                                                    switchMap((createReponse) => {
                                                        // dispatch an inline create service success action so the new service is in the state
                                                        this.store.dispatch(
                                                            ControllerServicesActions.inlineCreateControllerServiceSuccess(
                                                                {
                                                                    response: {
                                                                        controllerService: createReponse
                                                                    }
                                                                }
                                                            )
                                                        );

                                                        // fetch an updated property descriptor
                                                        return this.controllerServiceService
                                                            .getPropertyDescriptor(serviceId, descriptor.name, false)
                                                            .pipe(
                                                                take(1),
                                                                map((descriptorResponse) => {
                                                                    createServiceDialogReference.close();

                                                                    return {
                                                                        value: createReponse.id,
                                                                        descriptor:
                                                                            descriptorResponse.propertyDescriptor
                                                                    };
                                                                })
                                                            );
                                                    }),
                                                    catchError((error) => {
                                                        // TODO - show error
                                                        return NEVER;
                                                    })
                                                );
                                        })
                                    );
                                })
                            );
                    };

                    editDialogReference.componentInstance.editControllerService
                        .pipe(take(1))
                        .subscribe((payload: any) => {
                            this.store.dispatch(
                                ControllerServicesActions.configureControllerService({
                                    request: {
                                        id: request.controllerService.id,
                                        uri: request.controllerService.uri,
                                        payload
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ControllerServicesActions.selectControllerService({
                                    request: {
                                        processGroupId,
                                        id: serviceId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.configureControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.updateControllerService(request)).pipe(
                    map((response) =>
                        ControllerServicesActions.configureControllerServiceSuccess({
                            response: {
                                id: request.id,
                                controllerService: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ControllerServicesActions.controllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    configureControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.configureControllerServiceSuccess),
                tap(() => {
                    this.dialog.closeAll();
                })
            ),
        { dispatch: false }
    );

    promptControllerServiceDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.promptControllerServiceDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete Controller Service',
                            message: `Delete controller service ${request.controllerService.component.name}?`
                        },
                        panelClass: 'small-dialog'
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            ControllerServicesActions.deleteControllerService({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ControllerServicesActions.deleteControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.controllerServiceService.deleteControllerService(request)).pipe(
                    map((response) =>
                        ControllerServicesActions.deleteControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((error) =>
                        of(
                            ControllerServicesActions.controllerServicesApiError({
                                error: error.error
                            })
                        )
                    )
                )
            )
        )
    );

    selectControllerService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ControllerServicesActions.selectControllerService),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate([
                        '/process-groups',
                        request.processGroupId,
                        'controller-services',
                        request.id
                    ]);
                })
            ),
        { dispatch: false }
    );
}
